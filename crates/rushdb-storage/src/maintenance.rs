//! Retention, checkpoints, compaction, and storage diagnostics.
//! All fallible operations consult store health before accessing shared state.

use super::{
    receipt, wal, Op, Ordering, ReadLimits, ScanProjection, StorageError, StorageStatistics, Store,
    RETENTION_CURSOR_KEY,
};

/// Keys purged since the last compaction that make idle compaction worth
/// it even though the file did not grow (one small tenant's purge).
const PURGED_KEYS_TO_COMPACT: u64 = 10_000;

/// How long [`Store::compact_if_needed`] keeps retrying while a reader is
/// open — a pinned view or a backend read transaction
/// ([`crate::backend::COMPACT_BUSY`]) — and the pause between tries. The
/// compaction period is a multiple of the 5 s maintenance tick and both start
/// at boot, so a compaction tick lands while the tick's index-build step or
/// the text applier holds a short-lived view; one try per tick lost every
/// tick for 10 minutes after a purge (envelope `purge_half`, 2026-10-10).
const COMPACT_BUSY_RETRY: std::time::Duration = std::time::Duration::from_secs(2);
const COMPACT_BUSY_PAUSE: std::time::Duration = std::time::Duration::from_millis(20);

/// What [`Store::compact_if_needed`] did, so a deferral is never silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactOutcome {
    /// Compacted at this commit index.
    Compacted(u64),
    /// A commit landed within the quiescence window.
    CommitsRecent,
    /// Neither the growth nor the purged-keys threshold is met.
    BelowThreshold,
    /// A pinned reader (transaction, cursor, backup) is open.
    ReadersPinned,
    /// The store has no file (in-memory).
    NotDurable,
}

impl Store {
    /// Reclaim historical versions and ask redb to compact its copy-on-write
    /// pages. In-memory stores only perform logical MVCC reclamation.
    ///
    /// # Errors
    /// Returns [`StorageError::ReadersActive`] when protected read views are
    /// open, or another [`StorageError`] if reclamation or compaction fails.
    pub fn compact(&self) -> Result<u64, StorageError> {
        self.check_health()?;
        match self.compact_if_idle()? {
            Some(ci) => Ok(ci),
            None => Err(StorageError::ReadersActive),
        }
    }

    /// Signal a shutdown: in-flight maintenance scans stop within a batch so
    /// the store can be dropped (and redb closed cleanly) promptly.
    pub fn begin_shutdown(&self) {
        self.maintenance_abort.store(true, Ordering::Relaxed);
        for backend in self.all_backends() {
            backend.begin_shutdown();
        }
    }

    /// Whether [`Self::begin_shutdown`] has been called.
    #[must_use]
    pub fn maintenance_aborted(&self) -> bool {
        self.maintenance_abort.load(Ordering::Relaxed)
    }

    /// Make every shard's materialization durable (one redb `Immediate`
    /// commit with quick-repair state) without changing logical state. Under
    /// log-first, per-commit durability is the WAL fsync; redb commits are
    /// `Durability::None` until this checkpoint, which is also what lets WAL
    /// segments at or below the checkpointed LSN be truncated. A no-op for
    /// in-memory stores.
    ///
    /// # Errors
    /// Returns [`StorageError`] if a shard's checkpoint fails.
    pub fn checkpoint(&self) -> Result<(), StorageError> {
        self.check_health()?;
        let started = std::time::Instant::now();
        let durable_lsn = self.committed_version();
        self.for_each_backend_parallel(|backend| backend.checkpoint(durable_lsn))?;
        self.truncate_log(durable_lsn)?;
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
        self.checkpoint_wait_ms.fetch_add(
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(())
    }

    /// Run `copy` with every store file quiescent at a durable commit:
    /// publication is held, every published commit is checkpointed durably,
    /// and no backend commits (maintenance included) until `copy` returns.
    /// Readers continue; commits wait. `copy` receives that commit index,
    /// exactly what the copied files hold (a plain [`Self::checkpoint`]
    /// records `committed_version`, which can run ahead of what was applied).
    ///
    /// # Errors
    /// Returns [`StorageError`] if the checkpoint or a backend pause fails;
    /// `copy`'s own error is returned inside the `Ok`.
    pub fn with_quiescent_files<T>(
        &self,
        copy: impl FnOnce(u64) -> Result<T, String>,
    ) -> Result<Result<T, String>, StorageError> {
        fn paused(
            backends: &[std::sync::Arc<dyn crate::StorageBackend>],
            run: &mut dyn FnMut(),
        ) -> Result<(), String> {
            match backends.split_first() {
                None => {
                    run();

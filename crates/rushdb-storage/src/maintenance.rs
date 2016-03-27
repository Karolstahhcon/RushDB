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
                    Ok(())
                }
                Some((first, rest)) => {
                    let mut inner = Ok(());
                    first.while_writes_paused(&mut || inner = paused(rest, run))?;
                    inner
                }
            }
        }
        self.check_health()?;
        let _guard = self.health.guard(self.publish_lock.lock())?;
        self.drain_and_publish();
        let durable_lsn = self.committed_version();
        self.for_each_backend_parallel(|backend| backend.checkpoint(durable_lsn))?;
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
        let mut copy = Some(copy);
        let mut result = None;
        paused(&self.all_backends(), &mut || {
            if let Some(copy) = copy.take() {
                result = Some(copy(durable_lsn));
            }
        })
        .map_err(StorageError::Durability)?;
        Ok(result.unwrap_or_else(|| Err("store file copy did not run".into())))
    }

    /// Drop WAL segments fully covered by the durable materialization at
    /// `durable_lsn`, additionally clamped to the durable retention cursor
    ///: outcome-payload retention derives each old commit's
    /// payload key from the WAL's embedded command, so a segment must not be
    /// reclaimed before that sweep has consumed it, or the payload key
    /// becomes unrecoverable. A sealed segment whose last record is at or
    /// below `min(durable_lsn, retention_cursor)` is both recoverable from
    /// redb and already retention-consumed, so it can be reclaimed; the
    /// active segment is always retained.
    fn truncate_log(&self, durable_lsn: u64) -> Result<(), StorageError> {
        let cursor = self.retention_cursor()?;
        let target = durable_lsn.min(cursor);
        let mut guard = self.health.guard(self.log.lock())?;
        if let Some(log) = guard.as_mut() {
            // Make appended-but-unsynced records durable before reclaiming any
            // segment (`Async` durability relies on this checkpoint fsync).
            log.sync()
                .map_err(|error| StorageError::Durability(format!("WAL sync: {error}")))?;
            log.truncate_before(target.saturating_add(1))
                .map_err(|error| StorageError::Durability(format!("truncate WAL: {error}")))?;
        }
        Ok(())
    }

    /// The durable watermark up to which outcome-payload retention has
    /// consumed the WAL. `0` when retention has never run (or this
    /// store is non-durable), meaning no segment may yet be reclaimed on that
    /// basis — [`Self::truncate_log`]'s clamp.
    fn retention_cursor(&self) -> Result<u64, StorageError> {
        let Some(backend) = self.primary_backend() else {
            return Ok(0);
        };
        let view = backend.read_view()?;
        match view.read_value(
            RETENTION_CURSOR_KEY,
            0,
            true,
            ReadLimits::default(),
            ScanProjection::KeyValue,
        )? {
            Some(raw) => wal::decode_last_applied(&raw).map_err(|_| StorageError::CorruptLog),
            None => Ok(0),
        }
    }

    /// Durably advance the retention cursor to `lsn`. Applied directly
    /// against the primary backend (commit index `0`, like other
    /// out-of-band maintenance writes) — this is housekeeping, not a logical
    /// commit, so it never touches admission/publication.
    fn advance_retention_cursor(&self, lsn: u64) -> Result<(), StorageError> {
        let Some(backend) = self.primary_backend() else {
            return Ok(());
        };
        backend
            .apply(
                &[Op::Set(
                    RETENTION_CURSOR_KEY.to_vec(),
                    wal::encode_last_applied(0, lsn),
                )],
                0,
            )
            .map_err(StorageError::Durability)
    }

    /// Like [`Store::compact`], but a store with protected read views returns
    /// `Ok(None)` ("not now") instead of an error, so a background compactor
    /// can simply retry on the next tick. redb itself refuses to compact while
    /// any read transaction is live (`TransactionInProgress`); such a refusal
    /// surfaces as `Err` and is equally retryable by the caller.
    ///
    /// # Errors
    /// Returns [`StorageError`] if reclamation or compaction fails.
    ///
    /// Lock poisoning fences the store until reopen.
    pub fn compact_if_idle(&self) -> Result<Option<u64>, StorageError> {
        self.check_health()?;
        let _guard = self.health.guard(self.publish_lock.lock())?;
        if !self.health.guard(self.read_pins.lock())?.is_empty() {
            return Ok(None);
        }
        self.drain_and_publish();
        let commit_index = self.committed_version();
        self.retire_before_locked(commit_index)?;
        // Compact every shard file concurrently (independent files).
        self.for_each_backend_parallel(|backend| backend.compact())?;
        self.compactions.fetch_add(1, Ordering::Relaxed);
        Ok(Some(commit_index))
    }

    /// Idle compaction gated by quiescence and a growth ratio. This is what
    /// the background worker uses; its ticker is the rate limit (a cooldown
    /// here, stamped when a compaction finished, refused every other tick:
    /// effective period 2× the interval, measured 2026-10-10). It never
    /// rewrites the file while commits are flowing (quiescence), and only
    /// once the file has grown by `growth_percent` since the last
    /// compaction or at least [`PURGED_KEYS_TO_COMPACT`] keys were purged
    /// (a tenant purge frees space without growing the file). Returns why it
    /// deferred, or the commit index it compacted at.
    ///
    /// # Errors
    /// Returns [`StorageError`] if reclamation or compaction fails.
    ///
    /// Lock poisoning fences the store until reopen.
    pub fn compact_if_needed(
        &self,
        quiesce: std::time::Duration,
        growth_percent: u32,
    ) -> Result<CompactOutcome, StorageError> {
        self.check_health()?;
        let now = wal::now_ms();
        let quiesce_ms = u64::try_from(quiesce.as_millis()).unwrap_or(u64::MAX);
        let last_commit = self.last_commit_ms.load(Ordering::Relaxed);
        if last_commit != 0 && now.saturating_sub(last_commit) < quiesce_ms {
            return Ok(CompactOutcome::CommitsRecent);
        }
        let Some(file) = self.storage_file_bytes() else {
            return Ok(CompactOutcome::NotDurable);
        };
        let last_file = self.last_compacted_file_bytes.load(Ordering::Relaxed);
        let grown = last_file == 0
            || file.saturating_mul(100) >= last_file.saturating_mul(u64::from(growth_percent));
        // A purge frees pages without growing the file.
        let purged = self.purged_since_compaction.load(Ordering::Relaxed) >= PURGED_KEYS_TO_COMPACT;
        if !grown && !purged {
            return Ok(CompactOutcome::BelowThreshold);
        }
        let deadline = std::time::Instant::now() + COMPACT_BUSY_RETRY;
        let compacted = loop {
            let attempt = self.compact_if_idle();
            let reader_open = match &attempt {
                Ok(None) => true,
                Err(StorageError::Durability(error)) => error == crate::backend::COMPACT_BUSY,
                _ => false,
            };
            if reader_open && std::time::Instant::now() < deadline {
                // Retry with every lock released: waiting under the publish
                // or database lock would stall the reader we wait for.
                std::thread::sleep(COMPACT_BUSY_PAUSE);
                continue;
            }
            break attempt?;
        };
        match compacted {
            Some(ci) => {
                self.purged_since_compaction.store(0, Ordering::Relaxed);
                self.last_compacted_file_bytes
                    .store(self.storage_file_bytes().unwrap_or(file), Ordering::Relaxed);
                Ok(CompactOutcome::Compacted(ci))
            }
            None => Ok(CompactOutcome::ReadersPinned),
        }
    }

    /// Apparent size of the primary durable file, if this store is
    /// disk-backed. Operator metric only; not a durability or capacity input.
    ///
    /// Lock poisoning fences the store until reopen.
    pub fn storage_file_bytes(&self) -> Option<u64> {
        let path = self.health.cleanup(self.data_path.lock()).clone()?;
        std::fs::metadata(path).ok().map(|meta| meta.len())
    }

    /// Durable byte accounting by key family (see [`StorageStatistics`]).
    /// Aggregates every shard. Scans the key space once, so callers should
    /// sample it on a maintenance schedule, not per request.
    ///
    /// # Errors
    /// Returns [`StorageError::Durability`] if a shard cannot be read.
    ///
    /// Lock poisoning fences the store until reopen.
    pub fn storage_stats(&self) -> Result<StorageStatistics, StorageError> {
        self.check_health()?;
        // Every store aggregates its backend(s); the in-memory backend computes
        // the same accounting from its maps.
        let mut total = StorageStatistics::default();
        for backend in self.all_backends() {
            if self.maintenance_aborted() {
                return Err(StorageError::Cancelled);
            }
            let s = match backend.storage_stats() {
                Ok(stats) => stats,
                Err(error) if error == "maintenance cancelled" => {
                    return Err(StorageError::Cancelled);
                }
                Err(error) => return Err(StorageError::Durability(error)),
            };
            total.data_keys += s.data_keys;
            total.data_bytes += s.data_bytes;
            total.history_keys += s.history_keys;
            total.history_bytes += s.history_bytes;
            total.log_keys += s.log_keys;
            total.log_bytes += s.log_bytes;
            total.receipt_keys += s.receipt_keys;
            total.receipt_bytes += s.receipt_bytes;
        }
        total.admission_waits = self.admission_waits.load(Ordering::Relaxed);

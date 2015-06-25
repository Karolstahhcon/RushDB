//! WAL segment export and replay (CDC / PITR / incremental backup) over the
//! log-first segmented WAL (decision B: the log stores expanded op-sets).

use super::{wal, Ordering, StorageError, Store};
use crate::backend::StorageBackend;
use crate::log_store;
use std::sync::Arc;

impl Store {
    /// Open this store's segmented WAL and replay any tail beyond the shard
    /// materializations into the backends. Returns the log and the highest
    /// recovered commit index. The record header carries the routing shard and
    /// LSN, so replay is direct (no expander needed) and idempotent across a
    /// crash: a record at or below a shard's durable `LAST_APPLIED` is skipped.
    pub(crate) fn open_log_and_replay(
        &self,
        path: &std::path::Path,
        backends: &[Option<Arc<dyn StorageBackend>>],
        materialization: &[u64],
    ) -> Result<(log_store::LogStore, u64), StorageError> {
        // The WAL lives beside the redb file(s) as `<file>.wal/` so each store
        // has its own log (mirroring the `<file>.layout.json` convention).
        let wal_dir = {
            let mut os = path.as_os_str().to_os_string();
            os.push(".wal");
            std::path::PathBuf::from(os)
        };
        let mut log =
            log_store::LogStore::open(&wal_dir, 0, self.resource_config.log_segment_bytes)
                .map_err(|error| {
                    StorageError::Durability(format!("open WAL at {}: {error}", wal_dir.display()))
                })?;
        // A fresh WAL (no existing segments) beside a materialization that
        // already has a checkpoint — e.g. a store opened from a restored
        // base backup with no accompanying WAL directory — must continue
        // the commit sequence from that checkpoint, not restart at LSN 1
        // and collide with LSNs the materialization already has. A no-op
        // when the WAL has real history: see `seed_next_lsn_if_empty`.
        let highest_checkpoint = materialization.iter().copied().max().unwrap_or(0);
        log.seed_next_lsn_if_empty(highest_checkpoint.saturating_add(1));
        let wal_durable = log.durable_lsn();
        if wal_durable == 0 {
            return Ok((log, 0));
        }
        let from = materialization
            .iter()
            .copied()
            .min()
            .unwrap_or(0)
            .saturating_add(1);
        let records = log
            .read_from(from)
            .map_err(|error| StorageError::Durability(format!("read WAL for recovery: {error}")))?;
        let mut expected = from;
        // A chained commit (large-transaction support) spans several
        // consecutive physical records sharing one LSN; `ChainAssembler`
        // reconstructs the whole commit from them so it is applied exactly
        // once, atomically, never per-fragment (gap/skip detection below
        // operates on the reassembled commit's LSN, not each fragment's).
        let mut assembler = wal::ChainAssembler::default();
        let deriver = self.deriver();
        for (framed_lsn, raw) in records {
            let (frame_header, _) = wal::decode_wal_record(&raw).map_err(|error| {
                StorageError::Durability(format!("decode WAL record {framed_lsn}: {error}"))
            })?;
            if frame_header.lsn != framed_lsn {
                return Err(StorageError::Durability(format!(
                    "WAL recovery LSN mismatch: frame {framed_lsn}, header {}",
                    frame_header.lsn
                )));
            }
            let (header, body) = match assembler.feed(&raw, 0).map_err(|error| {
                StorageError::Durability(format!(
                    "reassemble chained WAL record {framed_lsn}: {error}"
                ))
            })? {
                wal::ChainFeed::Pending => continue,
                wal::ChainFeed::Complete(header, body) => (header, body),
            };
            let (_, ops, _command) = wal::decode_logical_body(header, &body).map_err(|error| {
                StorageError::Durability(format!("decode WAL record {framed_lsn}: {error}"))
            })?;
            if header.lsn < expected {
                continue; // already materialized on this shard
            }
            if header.lsn > expected {
                return Err(StorageError::Durability(format!(
                    "WAL recovery gap: expected LSN {expected}, found {}",
                    header.lsn
                )));
            }
            expected = header.lsn.saturating_add(1);
            let shard = usize::try_from(header.shard_id).map_err(|_| {
                StorageError::Durability(format!(
                    "WAL shard {} does not fit usize",
                    header.shard_id
                ))
            })?;
            let already = materialization.get(shard).copied().unwrap_or(0);
            if header.lsn <= already {
                continue;
            }
            if let Some(Some(backend)) = backends.get(shard) {
                if deriver.is_none() {
                    self.underived_replay.store(true, Ordering::SeqCst);
                }
                Self::apply_derived_replay(deriver.as_ref(), backend.as_ref(), &ops, header.lsn)?;
            }
        }
        // `LogStore::open` (above) already truncated away any trailing
        // incomplete chain from the segment files themselves, so every
        // record `read_from` can return must belong to a complete chain.
        // This is a correctness invariant, not a recoverable condition: if
        // it ever fires, recovery's own atomicity guarantee has a bug.
        debug_assert!(
            !assembler.is_open(),
            "WAL replay ended with an open chain; LogStore::open should have truncated it"
        );
        Ok((log, wal_durable))
    }

    /// Export the committed WAL as a checksummed segment stream (manifest +
    /// framed op-set records), bounded to the inclusive LSN range
    /// `[from_lsn, to_lsn]`. This is the CDC / PITR / incremental-backup cursor
    ///. Records come from the segmented WAL, which is the durable truth.
    ///
    /// # Errors
    /// Returns [`StorageError::Durability`] if the WAL cannot be read or the
    /// segment cannot be written.
    ///
    /// Lock poisoning fences the store until reopen.
    pub fn export_wal_segments<W: std::io::Write>(
        &self,
        writer: &mut W,
        from_lsn: u64,
        to_lsn: u64,
    ) -> Result<u64, StorageError> {
        self.check_health()?;
        let _guard = self.health.guard(self.publish_lock.lock())?;
        let mut records: Vec<(u64, Vec<u8>)> = {
            let guard = self.health.guard(self.log.lock())?;
            match guard.as_ref() {
                Some(log) => log.read_from(from_lsn).map_err(|error| {
                    StorageError::Durability(format!("read WAL for export: {error}"))
                })?,
                None => Vec::new(),
            }
        };
        records.retain(|(lsn, _)| *lsn <= to_lsn);
        // Fail closed: the segment must cover the requested inclusive range
        // `[from_lsn, to_lsn]` exactly, with no gaps. A missing record means
        // retention pruned it or the range is beyond the committed log;
        // certifying the export as complete would let a restore silently skip
        // commits. `from_lsn > to_lsn` is an empty range and needs no coverage.
        //
        // A chained commit (large-transaction support) contributes
        // several consecutive physical records sharing one LSN, so coverage
        // and contiguity are checked over the *distinct* logical LSNs, not
        // the raw physical record count — `records` is already in physical
        // (and therefore chain-grouped) order, so consecutive duplicates are
        // exactly one chain's fragments.
        if from_lsn <= to_lsn {

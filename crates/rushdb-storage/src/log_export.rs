//! Durable-log export: crash-log dumps and the streaming-replication
//! follower read path. Both derive the committed
//! command-batch sequence from the WAL's embedded-command envelope
//! rather than a separate journal.

use super::{prefix_range, wal, ReadLimits, ScanProjection, StorageError, Store, LOG_PREFIX};

impl Store {
    /// Durable-log export for tests/tools/backups.
    pub fn log_bytes(&self) -> Vec<Vec<u8>> {
        self.take_log_for_crash()
    }

    /// Export the decoded commit-batch payloads of the committed command log,
    /// ordered by LSN.
    ///
    /// Lock poisoning fences the store until reopen.
    pub fn take_log_for_crash(&self) -> Vec<Vec<u8>> {
        self.try_take_log_for_crash().unwrap_or_default()
    }

    /// Fallible crash-log export for recovery and archive callers. Unlike
    /// [`Store::take_log_for_crash`], this path never converts a scan,
    /// framing, or WAL decode failure into an empty/partial log.
    ///
    /// A durable store derives this from the WAL's embedded-command envelope
    ///; an in-memory store has no WAL file and keeps the row-based
    /// journal ([`log_key`], test mode only).
    ///
    /// # Errors
    /// Returns [`StorageError::CorruptLog`] for malformed WAL rows and the
    /// underlying storage error for failed backend scans.
    ///
    /// Lock poisoning fences the store until reopen.
    pub fn try_take_log_for_crash(&self) -> Result<Vec<Vec<u8>>, StorageError> {
        self.check_health()?;
        let _guard = self.health.guard(self.publish_lock.lock())?;
        if self.is_durable() {
            let records = {
                let guard = self.health.guard(self.log.lock())?;
                match guard.as_ref() {
                    Some(log) => log.read_from(1).map_err(|error| {
                        StorageError::Durability(format!("read WAL for crash log: {error}"))
                    })?,
                    None => Vec::new(),
                }
            };
            let mut out = Vec::with_capacity(records.len());
            // A chained commit (large-transaction support) spans
            // several physical records sharing one LSN; reassemble before
            // extracting its command, the same way WAL replay does.
            let mut assembler = wal::ChainAssembler::default();
            for (lsn, raw) in records {
                let (header, body) = match assembler
                    .feed(&raw, 0)
                    .map_err(|_| StorageError::CorruptLog)?
                {
                    wal::ChainFeed::Pending => continue,
                    wal::ChainFeed::Complete(header, body) => (header, body),

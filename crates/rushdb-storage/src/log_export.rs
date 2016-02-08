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

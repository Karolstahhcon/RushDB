//! Durable persistence of published commits: WAL append (+ fsync) followed by
//! the backend materialization, fanned out per physical shard.

use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(test)]
use super::Ordering;
use super::{
    derive, log_store, resources, wal, CommitPhase, Op, PendingCommit, StorageBackend,
    StorageError, Store,
};

impl Store {
    pub(super) fn persist_ops(
        &self,
        ops: &[Op],
        commit_index: u64,
        command_payload: Option<&[u8]>,
    ) -> Result<(), StorageError> {
        self.check_health()?;
        self.check_publisher()?;
        self.append_log(&[(commit_index, 0, ops, command_payload)])?;
        if let Some(backend) = self.primary_backend() {
            let before = derive::BackendRead(backend.read_view()?);
            let derived = Self::derive_ops(self.deriver().as_ref(), commit_index, ops, &before)?;
            drop(before);
            if let Err(error) = backend
                .apply(derived.as_deref().unwrap_or(ops), commit_index)
                .map_err(StorageError::Durability)
            {
                *self.health.guard(self.publisher_failure.lock())? = Some(error.clone());
                return Err(error);
            }
        }
        Ok(())
    }

    /// Append expanded op-sets to the segmented WAL and, under `Local`
    /// durability, fsync before returning. This is the durable acknowledgement
    /// boundary (Law 4); redb is a buffered materialization replayed from the
    /// WAL. A no-op for in-memory stores (no log). Each record's
    /// `command_payload`, when given, pairs its op-set with the originating
    /// `CommitBatch` so the command journal is WAL-derivable.
    ///
    /// A commit whose encoded op-set (plus command) exceeds the WAL's
    /// per-record ceiling is split into several framed records sharing one
    /// LSN (large-transaction support, see `wal::encode_chained_ops_record`)
    /// — transparent to every caller: `LogStore::append_batch` groups them
    /// back into one atomic unit, and reconstructs them as one commit on
    /// replay/replication. The overwhelmingly common case (everything fits
    /// in one record) produces byte-identical output to before.
    pub(super) fn append_log(
        &self,
        records: &[(u64, u32, &[Op], Option<&[u8]>)],
    ) -> Result<(), StorageError> {
        let encode_started = std::time::Instant::now();
        let encoded: Vec<(u64, Vec<u8>)> = records
            .iter()
            .flat_map(|(ci, shard, ops, command_payload)| {
                wal::encode_chained_ops_record(
                    *shard,
                    *ci,
                    ops,
                    *command_payload,
                    log_store::MAX_RECORD_BYTES,
                )
                .into_iter()
                .map(|framed| (*ci, framed))
            })
            .collect();
        self.observe_phase(CommitPhase::WalEncode, encode_started);
        let sync_started = std::time::Instant::now();
        let mut guard = self.health.guard(self.log.lock())?;
        if let Some(log) = guard.as_mut() {
            log.append_batch(&encoded)
                .map_err(|error| StorageError::Durability(format!("WAL append: {error}")))?;
            if self.resource_config.durability == resources::Durability::Local {
                log.sync()
                    .map_err(|error| StorageError::Durability(format!("WAL sync: {error}")))?;
            }
        }
        drop(guard);

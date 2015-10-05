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

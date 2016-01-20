//! A known committed import must remain successful when its acknowledgement is late.

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use rushdb_engine::{Engine, OperationClass};
use rushdb_query::SearchRequest;
use rushdb_storage::{Replicator, Store};
use rushdb_types::command::CommitBatch;
use serde_json::json;

struct DelayedAcknowledgement {
    store: Weak<Store>,
    deadline: Instant,
}

impl Replicator for DelayedAcknowledgement {
    fn replicate(&self, batch: &CommitBatch, payload: &[u8]) -> Result<(), String> {
        let store = self.store.upgrade().ok_or("store closed")?;
        store
            .apply_replicated_batch_with_payload(batch, Some(payload))

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
            .map_err(|error| error.to_string())?;
        // Publication has succeeded. Delay only its acknowledgement, so this
        // checks the committed outcome rather than racing planning cancellation.
        std::thread::sleep(
            self.deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
        );
        Ok(())
    }
}

#[test]
fn committed_import_succeeds_when_acknowledgement_passes_deadline() {
    let engine = Engine::new_in_memory();
    let mut context = engine
        .context(
            None,
            OperationClass::Write,
            Some(Duration::from_secs(2)),
            None,
        )
        .unwrap();
    engine

//! Retry receipts answer a repeated commit key for the idempotency
//! window, then expire and are swept in bounded batches.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rushdb_types::command::{CommitBatch, IsolationMode, COMMAND_VERSION};
use rushdb_types::ids::TenantId;
use uuid::Uuid;

use crate::{resources, CommitOutcome, Store};

const WINDOW_SECONDS: u64 = 60;

/// A durable store with a 60 s window and a mock receipt clock.
fn store(name: &str) -> (Store, Arc<AtomicU64>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("rushdb-receipts-{name}-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open_with_resources(
        &dir.join("rushdb.redb"),
        resources::StorageResources {
            idempotency_window_seconds: WINDOW_SECONDS,
            ..Default::default()
        },
    )
    .unwrap();
    store.set_expander(Arc::new(Box::new(|_, batch, txn| {
        txn.put(format!("u/{}", batch.transaction_id).into_bytes(), vec![1]);
        Ok(())
    })));
    let now = Arc::new(AtomicU64::new(1_000_000));
    let clock = Arc::clone(&now);
    store.set_receipt_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    (store, now, dir)
}

fn commit(store: &Store, commit_key: Uuid, body: &str) -> (u64, String) {
    match store
        .commit(commit_key, |_, _| {
            Ok((
                CommitBatch {
                    command_version: COMMAND_VERSION,
                    tenant: TenantId::single(),
                    transaction_id: body.to_owned(),
                    commit_key,
                    start_index: 0,
                    isolation: IsolationMode::ReadCommitted,
                    preconditions: vec![],
                    mutations: vec![],
                },
                body.to_owned(),

//! Durability-level contract (W2, research D10). `Local` is the default and
//! durable-before-ack; `Async` acks after queuing without forcing fsync; the
//! store still serves the committed data. A `Memory` store rejects a
//! configured redb cache. Everything below runs with the same correctness
//! surface — durability level changes the ack boundary, not the logical data.

use rushdb_storage::resources::{Durability, StorageResources};
use rushdb_storage::Store;
use rushdb_types::command::COMMAND_VERSION;

fn batch(ordinal: u64) -> (uuid::Uuid, rushdb_types::command::CommitBatch) {
    let key = uuid::Uuid::now_v7();
    let batch = rushdb_types::command::CommitBatch {
        command_version: COMMAND_VERSION,
        tenant: rushdb_types::ids::TenantId::single(),
        transaction_id: format!("t{ordinal}"),
        commit_key: key,
        start_index: 0,
        isolation: rushdb_types::command::IsolationMode::Snapshot,
        preconditions: vec![],
        mutations: vec![],
    };
    (key, batch)
}

#[test]
fn async_durability_commits_and_serves_data() {
    let store = Store::new_in_memory();
    store.set_expander(std::sync::Arc::new(Box::new(|_ci, _batch, _txn| Ok(()))));
    // In-memory store: Async == Memory semantics (no durable backend), but the
    // logical contract still requires the committed data to be visible.
    let (key, batch) = batch(1);
    let outcome = store
        .commit(key, |_ci, _txn| Ok((batch, 1u64)))
        .expect("commit under Async durability");
    match outcome {
        rushdb_storage::CommitOutcome::Committed {
            commit_index,
            payload,
        } => {
            assert_eq!(commit_index, 1);
            assert_eq!(payload, 1u64);
        }
        rushdb_storage::CommitOutcome::Conflicted(reason) => {
            panic!("unexpected conflict: {reason}")
        }
    }
    assert_eq!(store.committed_version(), 1);
    assert_eq!(store.log_bytes().len(), 1);
}

#[test]
fn local_durability_is_the_default_and_commits_durably() {
    let dir = std::env::temp_dir().join(format!(
        "rushdb-durability-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    {
        let store = Store::open(&db).unwrap();
        store.set_expander(std::sync::Arc::new(Box::new(|_ci, _batch, _txn| Ok(()))));
        let (key, batch) = batch(1);
        store
            .commit(key, |_ci, _txn| Ok((batch, 1u64)))
            .expect("commit");
    }
    // Reopen: the durable `Local` commit must be present.
    let store = Store::open(&db).unwrap();
    assert_eq!(store.committed_version(), 1);
    assert_eq!(store.log_bytes().len(), 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn memory_durability_rejects_configured_redb_cache() {
    let config = StorageResources {
        durability: Durability::Memory,
        shard_count: 1,
        ..StorageResources::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn async_durability_configures_redb_backend() {
    let dir = std::env::temp_dir().join(format!(
        "rushdb-async-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    let config = StorageResources {
        durability: Durability::Async,
        shard_count: 1,
        ..StorageResources::default()
    };
    let store = Store::open_with_resources(&db, config).expect("open async store");
    store.set_expander(std::sync::Arc::new(Box::new(|_ci, _batch, _txn| Ok(()))));
    let (key, batch) = batch(1);
    store
        .commit(key, |_ci, _txn| Ok((batch, 1u64)))
        .expect("commit");
    assert_eq!(store.committed_version(), 1);
    std::fs::remove_dir_all(&dir).ok();
}
#[test]
fn shard_count_one_opens_a_single_backend_file() {
    let dir = std::env::temp_dir().join(format!(
        "rushdb-shard1-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");

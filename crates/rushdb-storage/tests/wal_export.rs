//! Log-first WAL export/import integrity (decision B): the durable
//! segmented WAL stores checksummed framed op-set records, and a segment export
//! must round-trip those records so a restore replays the same state.

use rushdb_storage::{wal, Store};
use rushdb_types::command::{CommitBatch, COMMAND_VERSION};
use uuid::Uuid;

fn durable_store(name: &str) -> (std::path::PathBuf, Store) {
    let dir = std::env::temp_dir().join(format!(
        "rushdb-wal-{name}-{}-{}",
        std::process::id(),
        Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    let store = Store::open(&db).unwrap();
    store.set_expander(std::sync::Arc::new(Box::new(|_ci, _batch, _txn| Ok(()))));
    (dir, store)
}

fn commit_batch(ordinal: u64) -> CommitBatch {
    CommitBatch {
        command_version: COMMAND_VERSION,
        tenant: rushdb_types::ids::TenantId::single(),
        transaction_id: format!("t{ordinal}"),
        commit_key: Uuid::now_v7(),
        start_index: 0,
        isolation: rushdb_types::command::IsolationMode::Snapshot,
        preconditions: vec![],
        mutations: vec![],
    }
}

fn commit(store: &Store, ordinal: u64) {
    let batch = commit_batch(ordinal);
    store
        .commit(batch.commit_key, |_ci, _txn| Ok((batch.clone(), ordinal)))
        .expect("commit");
}

#[test]
fn exported_segments_round_trip_to_op_sets() {
    let (dir, store) = durable_store("round-trip");
    for i in 1..=5u64 {
        commit(&store, i);
    }

    // Export a bounded LSN range and verify the segment manifest + records.
    let mut out: Vec<u8> = Vec::new();
    let exported = store.export_wal_segments(&mut out, 2, 4).expect("export");
    assert_eq!(exported, 3);

    let mut reader = std::io::Cursor::new(&out);
    let Some((manifest, records)) = wal::read_wal_segment(&mut reader).expect("segment") else {
        panic!("expected one segment");
    };
    assert_eq!(manifest.from_lsn, 2);
    assert_eq!(manifest.to_lsn, 4);
    assert_eq!(manifest.record_count, 3);
    assert_eq!(records.len(), 3);

    // Every record decodes as a valid op-set with its own LSN.
    for (idx, record) in records.iter().enumerate() {
        let lsn = 2 + idx as u64;
        let (header, ops, _command) = wal::decode_ops_record(record).expect("decode");
        assert_eq!(header.lsn, lsn);
        assert!(!ops.is_empty(), "a commit produces at least LAST_APPLIED");
    }

    drop(store);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn incomplete_export_range_is_refused() {
    let (dir, store) = durable_store("incomplete");
    for i in 1..=3u64 {
        commit(&store, i);
    }

    // A requested range above the committed log (4,5 absent) must fail closed
    // rather than certify an incomplete segment.
    let mut out: Vec<u8> = Vec::new();
    let err = store.export_wal_segments(&mut out, 2, 5).unwrap_err();
    assert!(
        matches!(err, rushdb_storage::StorageError::Durability(_)),
        "{err:?}"
    );
    assert!(out.is_empty(), "no partial segment may be written");

    // A range whose start predates the log (0 absent) also fails closed.
    let mut out: Vec<u8> = Vec::new();
    assert!(store.export_wal_segments(&mut out, 0, 3).is_err());
    assert!(out.is_empty());

    // The exact covered range still succeeds.
    let mut out: Vec<u8> = Vec::new();
    assert_eq!(store.export_wal_segments(&mut out, 1, 3).unwrap(), 3);

    drop(store);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn replay_rejects_an_interior_lsn_gap() {
    let (dir, store) = durable_store("gap");
    for i in 1..=3u64 {
        commit(&store, i);

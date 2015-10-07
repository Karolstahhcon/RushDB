//! Durability gates for the redb-backed storage engine.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::reader::{self, try_access};
use rushdb_lmpg::Lmpg;
use rushdb_storage::{OutcomeKind, Store};
use rushdb_types::command::CommitBatch;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

const TENANT: TenantId = TenantId::from_ordinal(55);

fn path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rushdb-redb-{name}-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7()
    ))
}

fn scrub_clean(store: &Store) {
    let view = store.read_view().unwrap();
    let violations = rushdb_lmpg::scrub::scrub(&view, TENANT).unwrap();
    assert!(
        violations.is_empty(),
        "invariant violations: {violations:?}"
    );
}

#[test]
fn acknowledged_commits_and_idempotency_survive_reopen() {
    let dir = path("reopen");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    let first_commit_key;

    {
        let store = Arc::new(Store::open(&db).unwrap());
        let lmpg = Lmpg::new(store.clone(), TENANT);
        let first = lmpg
            .create_record("R", None, vec![("a".into(), FieldChange::Set(json!(1)))])
            .unwrap();
        first_commit_key = store
            .log_bytes()
            .first()
            .and_then(|raw| rushdb_types::command_codec::decode(raw).ok())
            .map(|batch| batch.commit_key)
            .unwrap();
        lmpg.update_fields(
            rushdb_lmpg::RecordRef {
                ordinal: first.ordinal,
                record_id: first.record_id,
            },
            vec![("a".into(), FieldChange::Set(json!(2)))],
        )
        .unwrap();
        lmpg.create_record("R", None, vec![("a".into(), FieldChange::Set(json!(3)))])
            .unwrap();
        assert_eq!(store.committed_version(), 3);
    }

    let store = Arc::new(Store::open(&db).unwrap());
    let lmpg = Lmpg::new(store.clone(), TENANT);
    assert_eq!(store.committed_version(), 3);
    assert_eq!(
        store.outcome_for(first_commit_key),
        Some(OutcomeKind::Committed(1))
    );
    let snap = lmpg.store.read_view().unwrap();
    assert_eq!(
        try_access::label_members(&snap, TENANT, "R").unwrap().len(),
        2
    );
    let first = rushdb_lmpg::reconstruct::reconstruct_record(&snap, TENANT, 0)
        .unwrap()
        .unwrap();
    assert_eq!(first.get("a"), Some(&json!(2)));
    scrub_clean(&store);
    drop(lmpg);
    drop(store);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn deletes_and_mvcc_history_survive_reopen() {
    let dir = path("delete-mvcc");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    let deleted;
    let before_delete;

    {
        let store = Arc::new(Store::open(&db).unwrap());
        let lmpg = Lmpg::new(store.clone(), TENANT);
        deleted = lmpg
            .create_record("Gone", None, vec![("v".into(), FieldChange::Set(json!(1)))])
            .unwrap();
        before_delete = store.committed_version();
        lmpg.delete_record(deleted).unwrap();
        assert!(
            try_access::label_members(&lmpg.store.read_view().unwrap(), TENANT, "Gone")
                .unwrap()
                .is_empty()
        );
    }

    let store = Arc::new(Store::open(&db).unwrap());
    let lmpg = Lmpg::new(store.clone(), TENANT);
    assert!(
        try_access::label_members(&lmpg.store.read_view().unwrap(), TENANT, "Gone")
            .unwrap()
            .is_empty()
    );
    let header = rushdb_types::keys::record_id_key(TENANT, &deleted.record_id);
    // The pre-delete snapshot must still resolve the header, while the latest
    // snapshot and live lookup must observe the deletion. This is the MVCC
    // history retention contract: reading at an older commit is stable.
    assert!(store
        .read_at(before_delete)
        .unwrap()
        .get(&header)
        .unwrap()
        .is_some());
    assert!(store.read_view().unwrap().get(&header).unwrap().is_none());
    assert!(store.try_get(&header).unwrap().is_none());
    scrub_clean(&store);
    drop(lmpg);
    drop(store);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn redb_compaction_preserves_graph_state() {
    let dir = path("compact");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");

    let store = Arc::new(Store::open(&db).unwrap());
    let lmpg = Lmpg::new(store.clone(), TENANT);
    let record = lmpg.create_record("Doc", None, vec![]).unwrap();
    for value in 0..25 {
        lmpg.update_fields(
            record,
            vec![("value".into(), FieldChange::Set(json!(value)))],
        )
        .unwrap();
    }
    let compacted_at = store.compact().unwrap();
    assert_eq!(store.gc_watermark(), compacted_at);
    drop(lmpg);
    drop(store);

    let reopened = Arc::new(Store::open(&db).unwrap());
    let lmpg = Lmpg::new(reopened.clone(), TENANT);
    let value = rushdb_lmpg::reconstruct::reconstruct_record(
        &lmpg.store.read_view().unwrap(),
        TENANT,
        record.ordinal,
    )
    .unwrap()
    .unwrap();
    assert_eq!(value.get("value"), Some(&json!(24)));
    assert_eq!(reopened.gc_watermark(), compacted_at);
    scrub_clean(&reopened);
    drop(lmpg);
    drop(reopened);
    std::fs::remove_dir_all(&dir).unwrap();
}

fn two_shard_resources() -> rushdb_storage::resources::StorageResources {
    rushdb_storage::resources::StorageResources {
        shard_count: 2,
        experimental_multi_shard: true,
        ..Default::default()
    }
}

fn tenants_on_distinct_shards() -> (TenantId, TenantId) {
    // Physical shard routing hashes the tenant id; probe until two tenants
    // land on different shards so the test actually exercises the fan-out.
    let first = TenantId::from_ordinal(1000);
    let first_shard = rushdb_storage::resources::physical_shard_for(Some(&first), 2);
    let second = (1001..1001 + 64)
        .map(TenantId::from_ordinal)
        .find(|t| rushdb_storage::resources::physical_shard_for(Some(t), 2) != first_shard)
        .expect("shard space has two shards");
    (first, second)
}

#[test]
fn parallel_shard_persist_keeps_both_shards_durable() {
    const PER_TENANT: usize = 25;
    // With two physical shards, concurrent commits to tenants on
    // distinct shards persist in parallel with a dense contiguous commit
    // index, and every acknowledged commit survives a reopen.
    //
    // Scope note: cross-shard *reads* are exercised by
    // `multi_shard_reads_merge_across_shards`; this test asserts the durable
    // surface — dense index, both shard files, reopen watermark — while
    // per-shard placement is gated deterministically in
    // `writer_tests::multi_shard_persist_fans_out_to_both_backends_in_parallel`.
    let (tenant_a, tenant_b) = tenants_on_distinct_shards();
    let dir = path("shards");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    {
        let store = Arc::new(Store::open_with_resources(&db, two_shard_resources()).unwrap());
        std::thread::scope(|scope| {
            for tenant in [tenant_a, tenant_b] {
                let store = store.clone();
                scope.spawn(move || {
                    let lmpg = Lmpg::new(store, tenant);
                    for i in 0..PER_TENANT {
                        lmpg.create_record(
                            "S",
                            None,
                            vec![("n".into(), FieldChange::Set(json!(i)))],
                        )
                        .unwrap();
                    }
                });
            }
        });
        assert_eq!(store.committed_version(), (2 * PER_TENANT) as u64);
        // Both physical shard files exist: the fan-out wrote past shard 0.
        assert!(db.exists(), "shard 0 file must exist");
        assert!(
            dir.join("rushdb.redb-1").exists(),
            "shard 1 file must exist after cross-shard commits"
        );
    }
    // Reopen: the durable watermark is the max LSN across shards — nothing
    // acknowledged is lost.
    let store = Arc::new(Store::open_with_resources(&db, two_shard_resources()).unwrap());
    assert_eq!(store.committed_version(), (2 * PER_TENANT) as u64);
    drop(store);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn multi_shard_crash_log_is_returned_in_commit_order() {
    // Shards are scanned in shard order, so a naive union of their log rows is
    // grouped by shard, not by commit index. The crash-log export must sort by
    // the LSN carried in each WAL frame's header.
    let (tenant_a, tenant_b) = tenants_on_distinct_shards();
    let dir = path("shard-log-order");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    let store = Arc::new(Store::open_with_resources(&db, two_shard_resources()).unwrap());
    store.set_expander(Arc::new(Box::new(|_ci, _batch, _txn| Ok(()))));

    let plan = [tenant_a, tenant_b, tenant_a, tenant_b, tenant_a];
    for (i, tenant) in plan.into_iter().enumerate() {
        let key = uuid::Uuid::now_v7();
        let batch = CommitBatch {
            command_version: rushdb_types::command::COMMAND_VERSION,
            tenant,
            transaction_id: format!("tx{i}"),
            commit_key: key,
            start_index: 0,
            isolation: rushdb_types::command::IsolationMode::ReadCommitted,
            preconditions: vec![],
            mutations: vec![],
        };
        store
            .commit_for_tenant(&tenant, key, move |_, _| Ok((batch, ())))
            .unwrap();
    }

    let log = store.try_take_log_for_crash().unwrap();
    let txids: Vec<String> = log
        .iter()
        .map(|payload| {
            rushdb_types::command_codec::decode(payload)
                .expect("log payload is an encoded commit batch")
                .transaction_id
        })
        .collect();
    assert_eq!(
        txids,
        vec!["tx0", "tx1", "tx2", "tx3", "tx4"],
        "crash log must follow commit order across shards"
    );

    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn historical_prefix_scan_reads_disk_backed_store() {
    // The RAM mirrors are not maintained for a redb-backed store, so the
    // memory candidate enumeration returned an empty scan. It must read
    // natively at the requested snapshot.
    let dir = path("hist-prefix");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    let store = Arc::new(Store::open(&db).unwrap());
    let commit = |value: u8| {
        store.set_expander(Arc::new(Box::new(move |_, _, txn| {
            txn.put(b"p/a".to_vec(), vec![value]);
            Ok(())
        })));
        let key = uuid::Uuid::now_v7();
        store
            .commit(key, move |_, _| {
                Ok((
                    CommitBatch {
                        command_version: rushdb_types::command::COMMAND_VERSION,
                        tenant: TENANT,
                        transaction_id: String::new(),
                        commit_key: key,
                        start_index: 0,
                        isolation: rushdb_types::command::IsolationMode::ReadCommitted,
                        preconditions: vec![],
                        mutations: vec![],
                    },
                    (),
                ))
            })
            .unwrap();
    };
    commit(1);
    commit(2);

    let at_one = store.try_range_prefix_at(b"p/", 1).unwrap();
    assert_eq!(at_one, vec![(b"p/a".to_vec(), vec![1])]);
    let at_two = store.try_range_prefix_at(b"p/", 2).unwrap();
    assert_eq!(at_two, vec![(b"p/a".to_vec(), vec![2])]);

    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

/// W4 phase 2: a multi-shard store serves reads by merging the native per-shard
/// views (no in-memory mirror). Both the query read path (`ReadView`) and the
/// commit-admission read path (`Txn`, used by the expander) must see data on
/// shards other than shard 0, in-process and after reopen.
#[test]
fn multi_shard_reads_merge_across_shards() {
    const PER_TENANT: usize = 25;
    let (tenant_a, tenant_b) = tenants_on_distinct_shards();
    let dir = path("shard-reads");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    {
        let store = Arc::new(Store::open_with_resources(&db, two_shard_resources()).unwrap());
        for tenant in [tenant_a, tenant_b] {
            let lmpg = Lmpg::new(store.clone(), tenant);
            for i in 0..PER_TENANT {
                lmpg.create_record("S", None, vec![("n".into(), FieldChange::Set(json!(i)))])
                    .unwrap();
            }
            // In-process read merges the native shard view. The count is only
            // correct if the commit path read the tenant's own shard too.
            let snap = lmpg.store.read_view().unwrap();
            assert_eq!(
                try_access::label_members(&snap, tenant, "S").unwrap().len(),
                PER_TENANT,
                "in-process read for {tenant:?}"
            );
        }
    }
    // Reopen: reads still merge across both shard files.
    let store = Arc::new(Store::open_with_resources(&db, two_shard_resources()).unwrap());
    assert_eq!(store.committed_version(), (2 * PER_TENANT) as u64);
    for tenant in [tenant_a, tenant_b] {
        let lmpg = Lmpg::new(store.clone(), tenant);
        let snap = lmpg.store.read_view().unwrap();
        assert_eq!(
            try_access::label_members(&snap, tenant, "S").unwrap().len(),
            PER_TENANT,
            "reopened read for {tenant:?}"
        );
        for ordinal in 0..PER_TENANT as u64 {
            let record = rushdb_lmpg::reconstruct::reconstruct_record(&snap, tenant, ordinal)
                .unwrap()
                .unwrap_or_else(|| {
                    panic!("tenant {tenant:?} ordinal {ordinal} missing after reopen")
                });
            assert_eq!(record.get("n"), Some(&json!(ordinal)));
        }
    }
    drop(store);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn concurrent_commits_on_one_store_stay_contiguous() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 25;
    // The cooperative group-commit publisher admits
    // concurrent writers and every commit lands exactly once with a dense
    // index — the baseline the 1/4/16/64-writer sweep measures against.
    let store = Arc::new(Store::new_in_memory());
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            let store = store.clone();
            scope.spawn(move || {
                let lmpg = Lmpg::new(store, TENANT);
                for i in 0..PER_THREAD {
                    lmpg.create_record("C", None, vec![("n".into(), FieldChange::Set(json!(i)))])
                        .unwrap();
                }
            });
        }
    });
    assert_eq!(
        store.committed_version(),
        (THREADS * PER_THREAD) as u64,
        "every concurrent commit must publish exactly once"
    );
    let lmpg = Lmpg::new(store.clone(), TENANT);
    let snap = lmpg.store.read_view().unwrap();
    assert_eq!(
        try_access::label_members(&snap, TENANT, "C").unwrap().len(),
        THREADS * PER_THREAD
    );
    scrub_clean(&store);
}

#[test]
fn commit_metadata_retention_prunes_log_but_preserves_state_and_outcomes() {
    // Retention retires old commits (bounded, resumable cursor advance)
    // without touching canonical state or retry receipts: receipts expire by
    // time, not by commit count, so an old retry inside the window
    // still discovers its durable outcome. The command journal
    // (`log_bytes`) is derived from the WAL, which retention does not
    // truncate (that is `checkpoint`'s job, clamped by the cursor).
    let dir = path("retention");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    let old_commit_key;
    {
        let store = Arc::new(Store::open(&db).unwrap());
        assert_eq!(store.retire_commit_metadata(50, 32).unwrap(), 0);
        let lmpg = Lmpg::new(store.clone(), TENANT);
        for i in 0..200 {
            lmpg.create_record("R", None, vec![("a".into(), FieldChange::Set(json!(i)))])
                .unwrap();
        }
        assert_eq!(store.committed_version(), 200);
        let log_before = store.log_bytes();
        assert_eq!(log_before.len(), 200, "one journal entry per commit");
        old_commit_key = rushdb_types::command_codec::decode(&log_before[0])
            .unwrap()
            .commit_key;
        let receipt_rows = store.storage_stats().unwrap().receipt_keys;
        assert_eq!(
            receipt_rows, 400,
            "one receipt plus its expiry row per commit"
        );

        let mut pruned = 0usize;
        loop {

use super::*;
use std::ops::Bound::{Excluded, Included, Unbounded};

#[test]
fn read_view_does_not_take_the_publication_lock() {
    // Opening a read view must serialize with GC on `read_pins`, not with the
    // writer's `publish_lock` (an in-flight fsync). Hold the publication lock
    // and prove a reader in another thread still opens within the deadline.
    let store = Arc::new(Store::new_in_memory());
    let publish = store.publish_lock.lock().unwrap();
    let worker_store = Arc::clone(&store);
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _ = tx.send(worker_store.read_view().is_ok());
    });
    let opened = rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap_or(false);
    drop(publish);
    handle.join().unwrap();
    assert!(opened, "read_view must not block on the publication lock");
}

fn configure(store: &Store) {
    store.set_expander(Arc::new(Box::new(|_, batch, txn| {
        let ops: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            serde_json::from_str(&batch.transaction_id).unwrap();
        for (key, value) in ops {
            match value {
                Some(value) => txn.put(key, value),
                None => txn.del(key),
            }
        }
        Ok(())
    })));
}

fn commit(store: &Store, ops: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>) -> u64 {
    let ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = ops.into_iter().collect();
    let key = Uuid::new_v4();
    match store
        .commit(key, |_, _| {
            Ok((
                CommitBatch {
                    command_version: rushdb_types::command::COMMAND_VERSION,
                    tenant: TenantId::from_ordinal(1),
                    transaction_id: serde_json::to_string(&ops).unwrap(),
                    commit_key: key,
                    start_index: 0,
                    isolation: rushdb_types::command::IsolationMode::ReadCommitted,
                    preconditions: vec![],
                    mutations: vec![],
                },
                (),
            ))
        })
        .unwrap()
    {
        CommitOutcome::Committed { commit_index, .. } => commit_index,
        CommitOutcome::Conflicted(reason) => panic!("unexpected conflicted: {reason}"),
    }
}

fn commit_tenant(store: &Store, tenant: TenantId, key: Vec<u8>) -> u64 {
    let commit_key = Uuid::new_v4();
    match store
        .commit(commit_key, |_, _| {
            Ok((
                CommitBatch {
                    command_version: rushdb_types::command::COMMAND_VERSION,
                    tenant,
                    transaction_id: serde_json::to_string(&[(key, Some(vec![1]))]).unwrap(),
                    commit_key,
                    start_index: 0,
                    isolation: rushdb_types::command::IsolationMode::ReadCommitted,
                    preconditions: vec![],
                    mutations: vec![],
                },
                (),
            ))
        })
        .unwrap_or_else(|error| panic!("tenant commit failed: {error:?}"))
    {
        CommitOutcome::Committed { commit_index, .. } => commit_index,
        CommitOutcome::Conflicted(reason) => panic!("unexpected conflicted: {reason}"),
    }
}

fn on_backends(test: impl Fn(&Store)) {
    let memory = Store::new_in_memory();
    configure(&memory);
    test(&memory);
    let path = crate::TempPath::store("read-view");
    let disk = Store::open(&path).unwrap();
    configure(&disk);
    test(&disk);
    drop(disk);
    crate::remove_store(&path).unwrap();
}

fn rows(view: &ReadView, bounds: ScanBounds, projection: ScanProjection) -> Vec<ScanRow> {
    let mut cursor = view.scan(bounds, projection).unwrap();
    let mut rows = Vec::new();
    loop {
        let batch = cursor.next_batch(2, 64).unwrap();
        assert!(batch.rows.len() <= 2 && batch.bytes <= 64);
        rows.extend(batch.rows);
        if batch.done {
            return rows;
        }
    }
}

#[test]
fn owned_read_views_outlive_store_on_both_backends() {
    fn owned_thread_safe<T: Send + Sync + 'static>() {}
    owned_thread_safe::<ReadView>();
    let path = crate::TempPath::store("owned-view");
    for persistent in [false, true] {
        let store = if persistent {
            Store::open(&path).unwrap()
        } else {
            Store::new_in_memory()
        };
        configure(&store);
        commit(&store, vec![(b"user/a".to_vec(), Some(vec![1]))]);
        let view = store.read_view().unwrap();
        drop(store);
        assert_eq!(view.get(b"user/a").unwrap(), Some(vec![1]));
        assert_eq!(
            rows(&view, ScanBounds::prefix(b"user/"), ScanProjection::Keys).len(),
            1
        );
        drop(view);
        if persistent {
            // Dropping the last owned view also releases the database lock.
            drop(Store::open(&path).unwrap());
        }
    }
    crate::remove_store(&path).unwrap();
}

#[test]
fn bounded_child_view_keeps_snapshot_and_has_independent_limits() {
    on_backends(|store| {
        commit(
            store,
            vec![
                (b"bounded/a".to_vec(), Some(vec![1, 2])),
                (b"bounded/b".to_vec(), Some(vec![3, 4])),
            ],
        );
        let parent = Arc::new(store.read_view().unwrap());
        let child = parent.with_materialization_limit(12);

        // The parent remains unrestricted when the child narrows its limits.
        assert_eq!(parent.get(b"bounded/a").unwrap(), Some(vec![1, 2]));
        let strict = parent.with_materialization_limit(10);
        assert!(matches!(
            strict.get(b"bounded/a"),
            Err(StorageError::ResourceExhausted(_))
        ));
        assert_eq!(child.commit_index(), parent.commit_index());

        // Both handles stay on the original pinned snapshot after a write.
        commit(store, vec![(b"bounded/a".to_vec(), Some(vec![9]))]);
        assert_eq!(parent.get(b"bounded/a").unwrap(), Some(vec![1, 2]));
        assert_eq!(child.get(b"bounded/a").unwrap(), Some(vec![1, 2]));

        let zero = parent.with_materialization_limit(0);
        assert!(matches!(
            zero.get(b"bounded/a"),
            Err(StorageError::ResourceExhausted(_))
        ));
        assert!(matches!(
            zero.range_prefix(b"bounded/"),
            Err(StorageError::ResourceExhausted(_))
        ));

        // Each row is 11 copied bytes; the 12-byte prefix cap permits one row
        // and rejects the next row before it is cloned.
        let prefix = parent.with_materialization_limit(12);
        assert!(matches!(
            prefix.range_prefix(b"bounded/"),
            Err(StorageError::ResourceExhausted(_))
        ));
    });
}

#[test]
fn native_cache_charge_outlives_store_until_last_view_closes() {
    let path = crate::TempPath::store("cache-charge");
    let config = resources::StorageResources {
        managed_limit_bytes: 1024 * 1024,
        page_cache_bytes: 128 * 1024,
        transaction_pool_bytes: 128 * 1024,
        durability: resources::Durability::Local,
        shard_count: 1,
        experimental_multi_shard: false,
        max_group_count: 0,
        max_group_bytes: 0,
        max_group_delay_ms: 0,
        logical_cache_bytes: 32 * 1024 * 1024,
        phase_budget_bytes: 1024 * 1024,
        query_memory_max_bytes: 0,
        lock_acquisition_timeout_ms: 0,
        log_segment_bytes: 64 * 1024 * 1024,
        max_expanded_commit_bytes: 512 * 1024 * 1024,
        ..resources::StorageResources::default()
    };
    let store = Store::open_with_resources(&path, config).unwrap();
    let budget = store.resource_budget();
    assert_eq!(budget.snapshot().used, config.page_cache_bytes);
    let view = store.read_view().unwrap();
    let second = store.read_view().unwrap();
    drop(store);
    assert_eq!(budget.snapshot().used, config.page_cache_bytes);
    drop(view);
    assert_eq!(budget.snapshot().used, config.page_cache_bytes);
    drop(second);
    assert_eq!(budget.snapshot().used, 0);
    crate::remove_store(&path).unwrap();
}

#[test]
fn tenant_read_view_opens_only_the_routed_shard() {
    let dir = crate::TempPath::dir("routed-view");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("rushdb.redb");
    let store = Store::open_with_resources(
        &path,
        resources::StorageResources {
            shard_count: 2,
            experimental_multi_shard: true,
            ..Default::default()
        },
    )
    .unwrap();
    let tenant = TenantId::from_ordinal(1_234);
    let all = store.read_view().unwrap();
    assert_eq!(all.native_backend_count(), 2);
    let routed = store.read_view_for_tenant(&tenant).unwrap();
    assert_eq!(routed.native_backend_count(), 1);
    assert_eq!(routed.commit_index(), all.commit_index());
    drop(routed);
    drop(all);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn tenant_read_view_keeps_global_snapshot_identity_across_shards() {
    let dir = crate::TempPath::dir("routed-ci");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("rushdb.redb");
    let store = Store::open_with_resources(
        &path,
        resources::StorageResources {
            shard_count: 2,
            experimental_multi_shard: true,
            ..Default::default()
        },
    )
    .unwrap();
    configure(&store);
    let first = TenantId::from_ordinal(1_000);
    let first_shard = resources::physical_shard_for(Some(&first), 2);
    let second = (1_001..1_065)
        .map(TenantId::from_ordinal)
        .find(|tenant| resources::physical_shard_for(Some(tenant), 2) != first_shard)
        .unwrap();
    commit_tenant(&store, first, b"first".to_vec());
    let global = commit_tenant(&store, second, b"second".to_vec());
    assert_eq!(store.committed_version(), global);
    let view = store.read_view_for_tenant(&first).unwrap();
    assert_eq!(view.commit_index(), global);
    drop(view);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn invalid_config_and_failed_open_do_not_leave_cache_charges() {
    let path = crate::TempPath::store("cache-failure");
    let invalid = resources::StorageResources {
        managed_limit_bytes: 1024,
        page_cache_bytes: 1024,
        transaction_pool_bytes: 1024,
        durability: resources::Durability::Local,
        shard_count: 1,
        experimental_multi_shard: false,
        max_group_count: 0,
        max_group_bytes: 0,
        max_group_delay_ms: 0,
        logical_cache_bytes: 32 * 1024 * 1024,
        phase_budget_bytes: 1024,
        query_memory_max_bytes: 0,
        lock_acquisition_timeout_ms: 0,
        log_segment_bytes: 64 * 1024 * 1024,
        max_expanded_commit_bytes: 512 * 1024 * 1024,
        ..resources::StorageResources::default()
    };
    assert!(matches!(
        Store::open_with_resources(&path, invalid),
        Err(StorageError::InvalidConfiguration(_))
    ));
    assert!(!path.exists());
    std::fs::create_dir(&path).unwrap();
    let store = Store::new_in_memory();
    assert!(store.attach_redb(&path).is_err());
    assert_eq!(store.resource_budget().snapshot().used, 0);
    std::fs::remove_dir(path).unwrap();
}

#[test]
fn current_views_are_repeatable_across_writes_deletes_and_recreates() {
    on_backends(|store| {
        let empty = store.read_view().unwrap();
        assert_eq!(empty.commit_index(), 0);
        commit(store, vec![(b"user/a".to_vec(), Some(vec![1]))]);
        let old = store.read_view().unwrap();
        commit(
            store,
            vec![
                (b"user/a".to_vec(), None),
                (b"user/b".to_vec(), Some(vec![2])),
            ],
        );
        let deleted = store.read_view().unwrap();
        commit(store, vec![(b"user/a".to_vec(), Some(vec![3]))]);
        assert_eq!(old.get(b"user/a").unwrap(), Some(vec![1]));
        assert_eq!(deleted.get(b"user/a").unwrap(), None);
        assert_eq!(store.read_at(2).unwrap().get(b"user/a").unwrap(), None);
        assert_eq!(
            store.read_at(1).unwrap().get(b"user/a").unwrap(),
            Some(vec![1])
        );
        assert_eq!(
            store.read_view().unwrap().get(b"user/a").unwrap(),
            Some(vec![3])
        );
        assert!(rows(&empty, ScanBounds::prefix(b"user/"), ScanProjection::Keys).is_empty());
        assert_eq!(
            rows(&old, ScanBounds::prefix(b"user/"), ScanProjection::Keys),
            vec![ScanRow {
                key: b"user/a".to_vec(),
                value: None
            }]
        );
    });
}

#[test]
fn read_pins_limit_gc_and_floor_is_monotonic_and_validated() {
    on_backends(|store| {
        commit(store, vec![(b"user/a".to_vec(), Some(vec![1]))]);
        let first = store.read_at(1).unwrap();
        let same = store.read_at(1).unwrap();
        commit(store, vec![(b"user/a".to_vec(), Some(vec![2]))]);
        commit(store, vec![(b"user/a".to_vec(), Some(vec![3]))]);
        store.gc(100).unwrap();
        assert_eq!(store.gc_watermark(), 1);
        drop(first);
        store.gc(3).unwrap();
        assert_eq!(store.gc_watermark(), 1);
        assert_eq!(same.get(b"user/a").unwrap(), Some(vec![1]));
        assert!(matches!(store.compact(), Err(StorageError::ReadersActive)));
        drop(same);
        store.gc(100).unwrap();
        assert_eq!(store.gc_watermark(), 3);
        assert!(matches!(
            store.read_at(2),
            Err(StorageError::SnapshotExpired {
                requested: 2,
                oldest: 3
            })
        ));
        assert!(matches!(
            store.read_at(4),
            Err(StorageError::FutureSnapshot {
                requested: 4,
                visible: 3
            })
        ));
        store.gc(0).unwrap();
        assert_eq!(store.gc_watermark(), 3);
    });
}

#[test]
fn scan_bounds_match_byte_order_oracle_for_arbitrary_prefixes() {
    on_backends(|store| {
        let mut keys = vec![
            vec![],
            vec![0],
            vec![0, 255],
            vec![1],
            vec![254],
            vec![255],
            vec![255, 0],
            vec![255, 255],
        ];
        for i in 0..=255 {
            keys.push(vec![42, i]);
        }
        commit(store, keys.iter().map(|k| (k.clone(), Some(vec![7]))));
        // Oracle: the store's own complete key set (native for a redb-backed
        // store, the maps for the in-memory store). Same operational keys as
        // the scan under test.
        let oracle_keys = store.try_range_prefix(&[]).unwrap();
        let view = store.read_view().unwrap();
        for prefix in [
            vec![],
            vec![0],
            vec![42],
            vec![255],
            vec![255, 255],
            vec![99],
        ] {
            // Empty prefix includes operational metadata too.
            let expected: Vec<ScanRow> = oracle_keys
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .map(|(key, value)| ScanRow {
                    key: key.clone(),
                    value: Some(value.clone()),
                })
                .collect();
            let mut cursor = view
                .scan(ScanBounds::prefix(&prefix), ScanProjection::KeyValue)
                .unwrap();
            let mut actual = Vec::new();
            loop {
                let batch = cursor.next_batch(3, 64 * 1024).unwrap();
                actual.extend(batch.rows);
                if batch.done {
                    break;
                }
            }
            assert_eq!(actual, expected);
        }
        for start in [Included(vec![0]), Excluded(vec![0]), Unbounded] {
            for end in [Included(vec![255]), Excluded(vec![255]), Unbounded] {
                let bounds = ScanBounds {
                    start: start.clone(),
                    end: end.clone(),
                };
                let actual = rows(&view, bounds, ScanProjection::Keys);
                let key_ok = |k: &Vec<u8>| -> bool {
                    let start_ok = match &start {
                        Included(s) => k >= s,
                        Excluded(s) => k > s,
                        Unbounded => true,
                    };
                    let end_ok = match &end {
                        Included(e) => k <= e,
                        Excluded(e) => k < e,
                        Unbounded => true,
                    };
                    start_ok && end_ok
                };
                let expected: Vec<ScanRow> = oracle_keys
                    .iter()
                    .filter(|(key, _)| key_ok(key))
                    .map(|(key, _)| ScanRow {
                        key: key.clone(),
                        value: None,
                    })
                    .collect();
                assert_eq!(actual, expected);
            }
        }
    });
}

#[test]
fn batch_boundaries_do_not_skip_rows_and_oversize_can_be_retried() {
    on_backends(|store| {
        commit(
            store,
            vec![
                (b"u/a".to_vec(), Some(vec![1; 5])),
                (b"u/b".to_vec(), Some(vec![2; 5])),
            ],
        );
        let view = store.read_view().unwrap();
        let mut cursor = view
            .scan(ScanBounds::prefix(b"u/"), ScanProjection::KeyValue)
            .unwrap();
        assert!(matches!(
            cursor.next_batch(10, 7),
            Err(StorageError::ResourceExhausted(_))
        ));
        let a = cursor.next_batch(10, 15).unwrap();
        assert_eq!(a.rows.len(), 1);
        assert_eq!(a.rows[0].key, b"u/a");
        assert_eq!(a.bytes, 8);
        let b = cursor.next_batch(10, 15).unwrap();
        assert_eq!(b.rows.len(), 1);
        assert_eq!(b.rows[0].key, b"u/b");
        assert!(b.done);
        assert!(cursor.next_batch(10, 15).unwrap().rows.is_empty());
    });
}

#[test]
fn keys_only_avoids_copying_oversized_current_values() {
    on_backends(|store| {
        commit(store, vec![(b"u/a".to_vec(), Some(vec![1; 100]))]);
        let view = store
            .read_with_limits(
                None,
                ReadLimits {
                    max_item_bytes: 10,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(matches!(
            view.get(b"u/a"),
            Err(StorageError::ResourceExhausted(_))
        ));
        assert_eq!(
            rows(&view, ScanBounds::prefix(b"u/"), ScanProjection::Keys),
            vec![ScanRow {
                key: b"u/a".to_vec(),
                value: None
            }]
        );
    });
}

#[test]
fn a_failed_batch_does_not_consume_undelivered_rows() {
    on_backends(|store| {
        commit(
            store,
            vec![
                (b"u/a".to_vec(), Some(vec![1])),
                (b"u/b".to_vec(), Some(vec![2; 100])),
            ],
        );
        let view = store
            .read_with_limits(
                None,
                ReadLimits {
                    max_item_bytes: 10,
                    ..Default::default()
                },
            )
            .unwrap();
        let mut cursor = view
            .scan(ScanBounds::prefix(b"u/"), ScanProjection::KeyValue)
            .unwrap();
        assert!(matches!(
            cursor.next_batch(10, 1024),
            Err(StorageError::ResourceExhausted(_))
        ));
        let delivered = cursor.next_batch(1, 1024).unwrap();
        assert_eq!(delivered.rows[0].key, b"u/a");
    });
}

#[test]
fn a_failed_batch_does_not_consume_undelivered_rows_across_multiple_prior_rows() {
    // v2's native-iterator fast path (B7 follow-up) cannot "un-consume" a
    // row the way v1's position-based retry does (re-seeking just
    // re-derives it) — it has its own undo buffer (`pending_native`) to
    // match the same contract. Two consumed-then-undelivered rows, not
    // just one, exercises that buffer actually holding more than a single
    // row, which `a_failed_batch_does_not_consume_undelivered_rows` alone
    // does not distinguish from "only ever handles exactly one".
    on_backends(|store| {
        commit(
            store,
            vec![
                (b"u/a".to_vec(), Some(vec![1])),
                (b"u/b".to_vec(), Some(vec![2])),
                (b"u/c".to_vec(), Some(vec![3; 100])),
            ],
        );
        let view = store

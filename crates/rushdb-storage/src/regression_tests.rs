use super::*;

#[test]
fn poisoned_storage_locks_fence_reads_writes_and_maintenance() {
    for lock in [
        "data",
        "history",
        "admission",
        "publisher",
        "pins",
        "pending",
    ] {
        let store = Store::new_in_memory();
        let view = store.read_view().unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match lock {
            "data" => {
                let _guard = store.data.write().unwrap();
                panic!("injected");
            }
            "history" => {
                let _guard = store.history.write().unwrap();
                panic!("injected");
            }
            "admission" => {
                let _guard = store.admission_lock.lock().unwrap();
                panic!("injected");
            }
            "publisher" => {
                let _guard = store.publish_lock.lock().unwrap();
                panic!("injected");
            }
            "pins" => {
                let _guard = store.read_pins.lock().unwrap();
                panic!("injected");
            }
            "pending" => {
                let _guard = store.pending.lock().unwrap();
                panic!("injected");
            }
            _ => unreachable!(),
        }));
        assert!(panic.is_err());
        assert!(
            matches!(store.read_view(), Err(StorageError::Unhealthy)),
            "{lock}"
        );
        assert!(
            matches!(view.get(b"key"), Err(StorageError::Unhealthy)),
            "{lock}"
        );
        assert!(
            matches!(
                store.record_aborted_outcome(Uuid::new_v4()),
                Err(StorageError::Unhealthy)
            ),
            "{lock}"
        );
        assert!(
            matches!(store.gc(0), Err(StorageError::Unhealthy)),
            "{lock}"
        );
        assert!(
            matches!(store.checkpoint(), Err(StorageError::Unhealthy)),
            "{lock}"
        );
        assert!(store.publisher_failure().unwrap().contains("unhealthy"));
        // Destruction must release resources even when lease accounting was
        // the source of the panic, with no second panic during unwinding.
        drop(view);
    }
}

#[test]
fn reopen_clears_process_local_poison_without_losing_durable_state() {
    let path = crate::TempPath::store("poison");
    {
        let store = Store::open(&path).unwrap();
        store
            .persist_ops_local(&[Op::Put(b"key".to_vec(), b"value".to_vec())], 0)
            .unwrap();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.admission_lock.lock().unwrap();
            panic!("injected request panic");
        }));
        assert!(matches!(store.read_view(), Err(StorageError::Unhealthy)));
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.read_view().unwrap().get(b"key").unwrap(),
        Some(b"value".to_vec())
    );
    assert!(store.publisher_failure().is_none());
    drop(store);
    crate::remove_store(&path).unwrap();
}

#[test]
fn admission_lock_acquisition_timeout_is_retryable() {
    let store = Store::new_with_resources(resources::StorageResources {
        lock_acquisition_timeout_ms: 1,
        ..resources::StorageResources::default()
    })
    .unwrap();
    // Hold the admission lock: a bounded acquisition must fail fast with a
    // retryable resource error instead of waiting forever (config #4).
    let held = store.admission_lock.lock().unwrap();
    let err = store.acquire_admission().unwrap_err();
    assert!(
        matches!(err, StorageError::ResourceExhausted(_)),
        "expected a retryable lock timeout, got {err:?}"
    );
    drop(held);
    assert!(store.acquire_admission().is_ok());
    let stats = store.storage_stats().unwrap();
    assert!(stats.admission_waits >= 1);
}

#[test]
fn checkpoint_work_is_accounted() {
    let store = Store::new_in_memory();
    store.checkpoint().unwrap();
    let stats = store.storage_stats().unwrap();
    assert_eq!(stats.checkpoints, 1);
}

#[test]
fn fallible_legacy_snapshot_reads_preserve_values() {
    let store = Store::new_in_memory();
    let key = b"snapshot/fallible".to_vec();
    store
        .persist_ops(&[Op::Put(key.clone(), b"value".to_vec())], 1, None)
        .unwrap();
    assert_eq!(store.try_get_at(&key, 1).unwrap(), Some(b"value".to_vec()));
    assert_eq!(
        store.try_range_prefix_at(b"snapshot/", 1).unwrap(),
        vec![(key, b"value".to_vec())]
    );
}

#[test]
fn storage_stats_honors_maintenance_cancellation() {
    let store = Store::new_in_memory();
    store
        .persist_ops(
            &(0u64..512)
                .map(|index| Op::Put(index.to_be_bytes().to_vec(), vec![1, 2, 3]))
                .collect::<Vec<_>>(),
            1,
            None,
        )
        .unwrap();
    store.begin_shutdown();
    assert!(matches!(
        store.storage_stats(),
        Err(StorageError::Cancelled)
    ));
}


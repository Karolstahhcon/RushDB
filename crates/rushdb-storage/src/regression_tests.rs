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

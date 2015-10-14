use super::*;
use std::time::Duration;

fn batch(commit_key: Uuid) -> CommitBatch {
    CommitBatch {
        command_version: rushdb_types::command::COMMAND_VERSION,
        tenant: TenantId::from_ordinal(1),
        transaction_id: "writer-test".into(),
        commit_key,
        start_index: 0,
        isolation: rushdb_types::command::IsolationMode::ReadCommitted,
        preconditions: vec![],
        mutations: vec![],
    }
}

#[test]
fn fallible_crash_log_export_preserves_decoded_batches() {
    let store = Store::new_in_memory();
    store.set_expander(Arc::new(Box::new(|_, _, txn| {
        txn.put(b"log-test".to_vec(), vec![1]);
        Ok(())
    })));
    let key = Uuid::new_v4();
    store
        .commit(key, |_, _| Ok((batch(key), ())))
        .expect("commit");
    assert_eq!(
        store.try_take_log_for_crash().unwrap(),
        store.take_log_for_crash()
    );
}

#[test]
fn tenant_routed_commit_rejects_batch_for_another_tenant() {
    let store = Store::new_in_memory();
    let routed = TenantId::from_ordinal(2);
    let key = Uuid::new_v4();
    let error = store
        .commit_for_tenant(&routed, key, |_, _| Ok((batch(key), ())))
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("batch tenant does not match the routed tenant"));
    assert_eq!(store.committed_version(), 0);
}

#[test]
fn expansion_failures_never_consume_commit_indices_in_any_entry_point() {
    for mode in 0..4 {
        let store = Store::new_in_memory();
        for missing in [true, false] {
            if !missing {
                store.set_expander(Arc::new(Box::new(|_, _, _| Err("bad expansion".into()))));
            }
            let key = Uuid::new_v4();
            let failed = match mode {
                0 => store.commit(key, |_, _| Ok((batch(key), ()))).map(|_| ()),
                1 => store
                    .commit_serializable(key, 0, DependencySet::default(), |_| Ok((batch(key), ())))
                    .map(|_| ()),
                2 => store.apply_replicated_batch(&batch(key)).map(|_| ()),
                // The transaction commit path must reserve its commit index
                // only after expansion succeeds; otherwise a user-reachable
                // expansion error strands the contiguous publisher forever.
                _ => store
                    .tx_commit(key, &TxAccumulator::new(TenantId::from_ordinal(1), 0))
                    .map(|_| ()),
            };
            assert!(failed.is_err());
            assert_eq!(store.next_commit_index.load(Ordering::SeqCst), 1);
            assert_eq!(store.committed_version(), 0);
        }
        store.set_expander(Arc::new(Box::new(|_, _, txn| {
            txn.put(b"a".to_vec(), vec![1]);
            Ok(())
        })));
        let key = Uuid::new_v4();
        assert!(matches!(
            store.commit(key, |_, _| Ok((batch(key), ()))),
            Ok(CommitOutcome::Committed {
                commit_index: 1,
                ..
            })
        ));
    }
}

#[test]
fn transaction_reads_resolve_at_their_pinned_snapshot() {
    // A cross-request transaction must not observe commits that land after
    // its BEGIN. The accumulator pins the base view; `tx_read` must resolve
    // through it instead of current committed state.
    let store = Store::new_in_memory();
    let base = Arc::new(store.read_at(0).unwrap());
    let mut acc = TxAccumulator::new(TenantId::from_ordinal(1), 0);
    acc.base_view = Some(base);

    store.set_expander(Arc::new(Box::new(|_, _, txn| {
        txn.put(b"k".to_vec(), vec![1]);
        Ok(())
    })));
    store
        .commit(Uuid::new_v4(), |_, _| Ok((batch(Uuid::new_v4()), ())))
        .unwrap();
    assert_eq!(store.try_get(b"k").unwrap(), Some(vec![1]));

    let seen = store
        .tx_read(&mut acc, |txn| txn.get(b"k").map_err(|e| e.to_string()))
        .unwrap();
    assert_eq!(
        seen, None,
        "transaction must read its pinned snapshot, not the later commit"
    );
}

#[test]
fn two_transactions_writing_one_key_cannot_both_commit() {
    // D01 regression: transaction validation must be serialized with
    // publication. Both accumulators share a write key and the same base
    // snapshot; exactly one may commit.
    let store = Store::new_in_memory();
    store.set_expander(Arc::new(Box::new(|_, _, txn| {
        txn.put(b"k".to_vec(), vec![1]);
        Ok(())
    })));
    let mut first = TxAccumulator::new(TenantId::from_ordinal(1), 0);
    first.buffer.insert(b"k".to_vec(), Some(vec![1]));
    let mut second = TxAccumulator::new(TenantId::from_ordinal(1), 0);
    second.buffer.insert(b"k".to_vec(), Some(vec![1]));

    assert!(store.tx_commit(Uuid::new_v4(), &first).is_ok());
    let second_outcome = store.tx_commit(Uuid::new_v4(), &second);
    assert!(
        second_outcome.is_err(),
        "second writer must observe the first commit as a conflict: {second_outcome:?}"
    );
    assert_eq!(store.committed_version(), 1);
    assert_eq!(store.next_commit_index.load(Ordering::SeqCst), 2);
}

#[test]
fn replication_watermark_is_optional_and_exposed_to_retention() {
    struct Watermark;
    impl Replicator for Watermark {
        fn replicate(&self, _batch: &CommitBatch, _payload: &[u8]) -> Result<(), String> {
            Ok(())
        }

        fn acknowledged_commit_index(&self) -> Option<u64> {
            Some(17)
        }
    }

    let store = Store::new_in_memory();
    assert_eq!(store.replication_acknowledged_commit(), None);
    store.set_replicator(Some(Arc::new(Watermark)));
    assert_eq!(store.replication_acknowledged_commit(), Some(17));
}

#[test]
fn persistence_failure_completes_all_waiters_and_fences_new_writes() {
    let store = Arc::new(Store::new_in_memory());
    store.fail_next_persist.store(true, Ordering::SeqCst);
    let (later_tx, later_rx) = mpsc::channel();
    store.pending.lock().unwrap().insert(
        2,
        PendingCommit {
            ci: 2,
            shard: 0,
            ops: vec![],
            prepared: PreparedCommand::from_ops(2, &[]),
            command_payload: Vec::new(),
            ack: later_tx,
        },
    );
    let (done_tx, done_rx) = mpsc::channel();
    let worker_store = store.clone();
    let worker = std::thread::spawn(move || {
        done_tx
            .send(worker_store.enqueue_and_publish(
                1,
                0,
                vec![Op::Put(b"a".to_vec(), vec![1])],
                vec![],
            ))
            .unwrap();
    });
    assert!(matches!(
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("publisher stranded a waiter"),
        Err(StorageError::Durability(_))
    ));
    assert!(matches!(
        later_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(StorageError::Durability(_))
    ));
    worker.join().unwrap();
    assert_eq!(store.committed_version(), 0);
    assert!(matches!(
        store.try_get(b"a"),
        Err(StorageError::Durability(_))
    ));
    assert!(store.pending.lock().unwrap().is_empty());
    assert!(store.publisher_failure().is_some());
    store.begin_shutdown();
    assert!(store.maintenance_aborted());
    assert!(matches!(
        store.commit::<(), _>(Uuid::new_v4(), |_, _| panic!("fenced writer ran a builder")),
        Err(StorageError::Durability(_))
    ));
    assert!(matches!(
        store.read_view(),
        Err(StorageError::Durability(_))
    ));
}

#[test]
fn temporary_nonpublisher_lock_holder_does_not_strand_a_commit() {
    let store = Arc::new(Store::new_in_memory());
    let guard = store.publish_lock.lock().unwrap();
    let (tx, rx) = mpsc::channel();
    let worker_store = store.clone();
    let worker = std::thread::spawn(move || {
        tx.send(worker_store.enqueue_and_publish(1, 0, vec![], vec![]))
            .unwrap();
    });
    let mut pending = store.pending.lock().unwrap();
    while !pending.contains_key(&1) {
        let (next, timeout) = store
            .pending_cv
            .wait_timeout(pending, Duration::from_secs(2))
            .unwrap();
        pending = next;
        assert!(!timeout.timed_out(), "worker failed to enqueue");
    }
    drop(pending);
    // No artificial notification when this read/maintenance lock is released.
    drop(guard);
    rx.recv_timeout(Duration::from_secs(2))
        .expect("non-publisher lock stranded commit")
        .unwrap();
    worker.join().unwrap();
}

#[test]
fn actual_write_summaries_detect_conflicts_without_reexpansion() {
    let store = Store::new_in_memory();
    let expansions = Arc::new(AtomicU64::new(0));
    let observed = expansions.clone();
    store.set_expander(Arc::new(Box::new(move |_, _, txn| {
        observed.fetch_add(1, Ordering::SeqCst);
        // Re-expanding against current state would incorrectly emit no write.
        if txn.get(b"a").map_err(|e| e.to_string())?.is_none() {
            txn.put(b"a".to_vec(), vec![1]);
        }
        Ok(())
    })));
    let first = Uuid::new_v4();
    store.commit(first, |_, _| Ok((batch(first), ()))).unwrap();
    let mut deps = DependencySet::default();
    deps.record(b"a".to_vec(), b"a".to_vec());
    let next = Uuid::new_v4();
    assert!(matches!(
        store.commit_serializable(next, 0, deps, |_| Ok((batch(next), ()))),
        Ok(CommitOutcome::Conflicted(_))
    ));
    assert_eq!(expansions.load(Ordering::SeqCst), 1);
    assert_eq!(store.outcome_for(next), Some(OutcomeKind::Aborted));
}

#[test]
fn prepared_commands_detect_read_write_and_write_write_overlap() {
    let mut reader = PreparedCommand::from_ops(1, &[Op::Put(b"out".to_vec(), vec![1])]);
    reader.reads.record(b"input".to_vec(), b"input".to_vec());
    let writer = PreparedCommand::from_ops(2, &[Op::Set(b"input".to_vec(), vec![2])]);
    assert!(reader.conflicts_with(&writer));

    let same_key = PreparedCommand::from_ops(3, &[Op::Del(b"out".to_vec())]);
    assert!(reader.conflicts_with(&same_key));

    let disjoint = PreparedCommand::from_ops(4, &[Op::Put(b"other".to_vec(), vec![3])]);
    assert!(!reader.conflicts_with(&disjoint));
    assert_eq!(reader.commit_index, 1);

    let mut whole = PreparedCommand::from_ops(5, &[]);

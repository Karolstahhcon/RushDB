//! Differential safety net for the C-1 sequencer.
//!
//! The sequencer's premise is that a command can be expanded against the
//! committed base plus the *pending* ops of lower-index commits (through
//! `Txn`'s overlay) instead of waiting for those commits to be published. These
//! tests assert that claim byte-for-byte: the overlay expansion must equal the
//! serialized forward path, which expands each command only after its
//! predecessors are committed, and the accumulated overlay must match the
//! really committed state.

use super::*;
use rushdb_types::command::Mutation;

/// Read-modify-write expander. Each `PutRawKey { key, value }` adds the
/// big-endian `i64` delta in `value` to whichever value the transaction view
/// currently holds for `key` (committed base overlaid with prior pending ops)
/// and writes the sum. Reading before writing is exactly the shape the
/// sequencer's overlay has to satisfy.
fn summing_expander() -> Arc<ExpandFn> {
    Arc::new(Box::new(|_ci, batch, txn| {
        for mutation in &batch.mutations {
            let Mutation::PutRawKey { key, value } = mutation else {
                continue;
            };
            let delta = i64::from_be_bytes(
                value
                    .as_slice()
                    .try_into()
                    .map_err(|_| "delta must be 8 bytes".to_string())?,
            );
            let current = txn
                .get(key)
                .map_err(|error| error.to_string())?
                .map_or(0, |raw| {
                    raw.as_slice().try_into().map_or(0, i64::from_be_bytes)
                });
            txn.put(key.clone(), (current + delta).to_be_bytes().to_vec());
        }
        Ok(())
    }))
}

fn delta_batch(commit_key: Uuid, tenant: TenantId, deltas: &[(Vec<u8>, i64)]) -> CommitBatch {
    CommitBatch {
        command_version: rushdb_types::command::COMMAND_VERSION,
        tenant,
        transaction_id: format!("seq-{commit_key}"),
        commit_key,
        start_index: 0,
        isolation: rushdb_types::command::IsolationMode::Snapshot,
        preconditions: vec![],
        mutations: deltas
            .iter()
            .map(|(key, delta)| Mutation::PutRawKey {
                key: key.clone(),
                value: delta.to_be_bytes().to_vec(),
            })
            .collect(),
    }
}

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// A deterministic mixed workload: a fixed read-modify-write prefix over a
/// small account set followed by a seeded pseudo-random sequence. Repeated keys
/// within one command and overlapping accounts across commands force the
/// overlay to carry state forward correctly.
fn workload() -> Vec<Vec<(Vec<u8>, i64)>> {
    let mut steps: Vec<Vec<(Vec<u8>, i64)>> = vec![
        vec![(b"acct/a".to_vec(), 5)],
        vec![(b"acct/a".to_vec(), -2), (b"acct/b".to_vec(), 7)],
        vec![(b"acct/b".to_vec(), 3), (b"acct/b".to_vec(), 1)],
        vec![(b"acct/c".to_vec(), 9)],
    ];
    let accounts = [b"acct/a".to_vec(), b"acct/b".to_vec(), b"acct/c".to_vec()];
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for _ in 0..28 {
        let count = 1 + usize::try_from(xorshift(&mut state) % 3).unwrap_or(0);
        let mut step = Vec::with_capacity(count);
        for _ in 0..count {
            let index = usize::try_from(xorshift(&mut state) % 3).unwrap_or(0);
            let delta = i64::try_from(xorshift(&mut state) % 7).unwrap_or(0) - 3;
            step.push((accounts[index].clone(), delta));
        }
        steps.push(step);
    }
    steps
}

fn op_key(op: &Op) -> &[u8] {
    match op {
        Op::Put(key, _) | Op::Set(key, _) | Op::Del(key) | Op::Purge(key) => key,
    }
}

/// Fold the data (non-coordination) effect of an op-set into the pending
/// overlay, exactly as the sequencer will between commands.
fn apply_net(pending: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>, ops: &[Op]) {
    for op in ops {
        if is_coordination_write(op_key(op)) {
            continue;
        }
        match op {
            Op::Put(key, value) | Op::Set(key, value) => {
                pending.insert(key.clone(), Some(value.clone()));
            }
            Op::Del(key) | Op::Purge(key) => {
                pending.insert(key.clone(), None);
            }
        }
    }
}

#[test]
fn sequencer_buffer_trims_published_and_tracks_in_flight() {
    let store = Store::new_in_memory();
    store.set_expander(summing_expander());
    let tenant = TenantId::from_ordinal(1);

    let first = Uuid::new_v4();
    let batch = delta_batch(first, tenant, &[(b"acct".to_vec(), 5)]);
    store
        .commit(first, |_, _| Ok((batch.clone(), ())))
        .expect("first commit");
    // The just-published entry stays in the buffer until the next critical
    // section trims it (the base then already contains it).
    {
        let state = store.admission_lock.lock().unwrap();
        assert_eq!(
            state.buffer.get(b"acct".as_slice()).map(|(ci, _)| *ci),
            Some(1)
        );
    }

    let second = Uuid::new_v4();
    let batch = delta_batch(second, tenant, &[(b"acct".to_vec(), 3)]);
    store
        .commit(second, |_, _| Ok((batch.clone(), ())))
        .expect("second commit");
    {
        let state = store.admission_lock.lock().unwrap();
        assert_eq!(
            state.buffer.get(b"acct".as_slice()).map(|(ci, _)| *ci),
            Some(2),
            "the published entry is trimmed and the new in-flight one recorded"
        );
    }
}

#[test]
fn concurrent_commits_coalesce_into_group_commits() {
    // C-1 payoff: releasing admission/shard before the durability wait lets
    // concurrent commits accumulate in `pending` so one `drain_and_publish`
    // persists many of them. A linger widens the join window deterministically.
    let config = resources::StorageResources {
        max_group_delay_ms: 20,
        ..Default::default()
    };
    let store = Arc::new(Store::new_with_resources(config).unwrap());
    // Each commit writes a distinct key (so the publisher's write-write barrier
    // does not split the group) and reads nothing (dependency-free).
    store.set_expander(Arc::new(Box::new(|_ci, batch, txn| {
        let mut key = b"key/".to_vec();
        key.extend_from_slice(batch.commit_key.as_bytes());
        txn.put(key, vec![1]);
        Ok(())
    })));
    let tenant = TenantId::from_ordinal(1);
    let threads = 8usize;
    let per_thread = 50usize;
    let barrier = Arc::new(std::sync::Barrier::new(threads));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..per_thread {
                let commit_key = Uuid::new_v4();
                let batch = delta_batch(commit_key, tenant, &[]);
                store
                    .commit(commit_key, |_, _| Ok((batch.clone(), ())))
                    .expect("concurrent commit");
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let total = (threads * per_thread) as u64;
    assert_eq!(store.committed_version(), total);
    let groups = store.group_commits.load(Ordering::Relaxed);
    assert!(
        groups < total,
        "expected coalescing: {groups} group-commits for {total} commits"
    );
}

/// Read-dependent commits of one tenant (every one reads and
/// rewrites the same counter) expanded against the sequencer overlay must
/// still coalesce into shared publish groups — before, the publisher barrier
/// gave each its own fsync — and the counter must stay exact.
#[test]
fn dependent_sequenced_commits_share_publish_groups_and_stay_exact() {
    let config = resources::StorageResources {
        max_group_delay_ms: 20,
        ..Default::default()
    };
    let store = Arc::new(Store::new_with_resources(config).unwrap());
    store.set_expander(summing_expander());
    let tenant = TenantId::from_ordinal(1);
    let threads = 8usize;

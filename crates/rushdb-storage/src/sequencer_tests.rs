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

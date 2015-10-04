//! Apply-time derivation at the storage layer, with a test deriver
//! that keeps one counter row per base key prefix.

use super::*;
use rushdb_types::command::Mutation;

const COUNT_PREFIX: &[u8] = b"count/";

/// Counts live `base/<group>/…` keys per group in `count/<group>`: a
/// read-before-write derivation, like presence and dictionary refcounts.
struct CountingDeriver;

fn group_of(key: &[u8]) -> Option<&[u8]> {
    let rest = key.strip_prefix(b"base/")?;
    let end = rest.iter().position(|byte| *byte == b'/')?;
    Some(&rest[..end])
}

impl Deriver for CountingDeriver {
    fn derive(
        &self,
        _ci: u64,
        ops: &[Op],
        before: &dyn DeriveRead,
    ) -> Result<Vec<Op>, StorageError> {
        let mut seen: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut deltas: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
        for op in ops {
            let (key, live) = match op {
                Op::Put(key, _) | Op::Set(key, _) => (key, true),
                Op::Del(key) | Op::Purge(key) => (key, false),
            };
            let Some(group) = group_of(key) else {
                continue;
            };
            let existed = match seen.get(key) {
                Some(state) => state.is_some(),
                None => before.get(key)?.is_some(),
            };
            seen.insert(key.clone(), live.then(Vec::new));
            let delta = i64::from(live) - i64::from(existed);
            if delta != 0 {
                let mut counter = COUNT_PREFIX.to_vec();
                counter.extend_from_slice(group);
                *deltas.entry(counter).or_default() += delta;
            }
        }
        let mut out = Vec::new();
        for (counter, delta) in deltas {
            let current = before.get(&counter)?.map_or(0, |raw| {
                i64::from_be_bytes(raw.as_slice().try_into().unwrap_or([0; 8]))
            });
            out.push(Op::Put(counter, (current + delta).to_be_bytes().to_vec()));
        }
        Ok(out)
    }

    fn is_derived_key(&self, key: &[u8]) -> bool {
        key.starts_with(COUNT_PREFIX)
    }

    fn prefix_reaches_derived(&self, prefix: &[u8]) -> bool {
        COUNT_PREFIX.starts_with(prefix) || prefix.starts_with(COUNT_PREFIX)
    }
}

fn put_expander() -> Arc<ExpandFn> {
    Arc::new(Box::new(|_ci, batch, txn| {
        for mutation in &batch.mutations {
            match mutation {
                Mutation::PutRawKey { key, value } => txn.put(key.clone(), value.clone()),
                Mutation::DeleteRawKey { key } => txn.del(key.clone()),

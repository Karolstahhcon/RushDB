use std::collections::{BTreeMap, BTreeSet};

use rushdb_storage::{Deriver as _, StorageError, Txn};
use rushdb_types::command::{CommitBatch, Mutation};
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{
    ann_manifest_meta_prefix, emb_alias_key, emb_bindings_prefix, emb_reclaim_key,
    emb_reclaim_prefix, emb_reclaim_value, emb_vector_key, key, parse_emb_reclaim_value,
    security_generation_key, security_policy_key, security_prefix, u64be, vector_index_meta_prefix,
    P_EMB_VECTOR, P_META,
};
use rushdb_types::occurrence::{
    decode_occ_value, encode_occ_value, OccurrenceRow, PropertyDefinition,
};

use crate::reader::keys_ext as kx;

const ADJ_OUT: &[u8] = b"out";
const ADJ_IN: &[u8] = b"in";

/// Maximum embedding-slot reclamations erased in one commit; the driver loops
/// commits so the batch stays bounded.
const EMB_SLOT_RECLAIM_BATCH: usize = 512;

/// Key of the named per-tenant allocation counter in the meta keyspace.
#[must_use]
pub fn meta_counter_key(tenant: TenantId, name: &str) -> Vec<u8> {
    key(tenant, &[P_META, b"alloc", name.as_bytes()])
}

/// Bounded set of the smallest remaining purgeable keys for one purge chunk:
/// tenant keys excluding the security subrange (data purge must never erase
/// enforcement) and the usage-bytes row, then tenant-owned vector/ANN metadata stored outside the
/// binary tenant keyspace. Shared by the chunk expansion and the chunk driver
/// so both agree on when a purge is complete.
pub(crate) fn purge_batch_keys(
    txn: &Txn,
    t: TenantId,
    max_keys: usize,
) -> Result<Vec<Vec<u8>>, StorageError> {
    let mut out = txn.keys_prefix_limited(&key(t, &[]), max_keys)?;
    // The usage-bytes row stays too: it is the tenant's running total, and the
    // purged rows are subtracted from it as they go.
    out.retain(|k| !k.starts_with(&security_prefix(t)) && !rushdb_storage::is_usage_bytes_key(k));
    // Only reach for external metadata once tenant data is exhausted, so the
    // driver cannot park on metadata while tenant data remains.
    if out.len() < max_keys {
        for external in [vector_index_meta_prefix(t), ann_manifest_meta_prefix(t)] {
            if out.len() >= max_keys {
                break;
            }
            let take = max_keys - out.len();
            out.extend(txn.keys_prefix_limited(external.as_bytes(), take)?);
        }
    }
    Ok(out)
}

/// Deterministic derivation of every canonical and derived key implied by a
/// committed batch. Runs on the forward publish path and again during crash
/// recovery replay; pure with respect to (state, batch).
///
/// # Errors
/// Returns [`StorageError`] if the batch targets an unsupported command
/// version, a mutation is internally inconsistent with stored state, or a
/// write fails.
#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive match over Mutation variants is the single source of truth for key layout and must stay in one deterministic sequence"
)]
pub fn expand_commit(ci: u64, batch: &CommitBatch, txn: &mut Txn) -> Result<(), StorageError> {
    if batch.command_version != rushdb_types::command::COMMAND_VERSION {
        return Err(StorageError::UnsupportedCommandVersion(
            batch.command_version,
        ));
    }
    let t = batch.tenant;
    // Records touched by this commit, with the labels named by its header
    // mutations (a retype or delete leaves the old label's text indexes).
    let mut text_owners: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    // Records whose place in the graph this commit changed (structural
    // indexes): created, deleted or retyped records, and the endpoints of
    // created or deleted relationships.
    let mut topology: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for m in &batch.mutations {
        match m {
            Mutation::SetSecurityPolicy { policy, mode }
            | Mutation::SetSecurityPolicyThreshold { policy, mode, .. } => {
                use rushdb_types::security::{policy_fingerprint_with_min_count, ActivePolicy};
                let min_count = match m {
                    Mutation::SetSecurityPolicyThreshold { min_count, .. } => *min_count,
                    _ => 0,
                };
                let generation_key = security_generation_key(t);
                if let Some(raw) = txn.get(&generation_key)? {
                    let previous = u64::from_be_bytes(
                        raw.as_slice()
                            .try_into()
                            .map_err(|_| StorageError::CorruptLog)?,
                    );
                    if previous >= ci {
                        return Err(StorageError::CorruptLog);
                    }
                }
                let active = ActivePolicy {
                    generation: ci,
                    policy: policy.clone(),
                    mode: *mode,
                    fingerprint: policy_fingerprint_with_min_count(policy, *mode, min_count),
                    min_count,
                };
                txn.put(generation_key, ci.to_be_bytes().to_vec());
                txn.put(
                    security_policy_key(t),
                    serde_json::to_vec(&active).map_err(ser_err)?,
                );
            }
            Mutation::CreateRecordHeader {
                record_id,
                ordinal,
                label,
            } => {
                text_owners
                    .entry(*ordinal)
                    .or_default()
                    .insert(label.clone());
                topology.entry(*ordinal).or_default().insert(label.clone());
                txn.put(
                    rushdb_types::keys::record_id_key(t, record_id),
                    u64be(*ordinal).to_vec(),
                );
                txn.put(
                    rushdb_types::keys::record_hdr_key(t, *ordinal),
                    rushdb_types::keys::encode_record_hdr(record_id, label),
                );
                txn.put(
                    rushdb_types::keys::label_member_key(t, label, *ordinal),
                    Vec::new(),
                );
                ensure_counter(txn, t, "record", ordinal + 1)?;
            }
            Mutation::DeleteRecordHeader {
                record_id,
                ordinal,
                label,
            } => {
                text_owners
                    .entry(*ordinal)
                    .or_default()
                    .insert(label.clone());
                topology.entry(*ordinal).or_default().insert(label.clone());
                txn.del(rushdb_types::keys::record_id_key(t, record_id));
                txn.del(rushdb_types::keys::record_hdr_key(t, *ordinal));
                txn.del(rushdb_types::keys::label_member_key(t, label, *ordinal));
            }
            Mutation::RetypeRecordHeader {
                record_id,
                ordinal,
                from_label,
                to_label,
            } => {
                for owners in [&mut text_owners, &mut topology] {
                    let held = owners.entry(*ordinal).or_default();
                    held.insert(from_label.clone());
                    held.insert(to_label.clone());
                }
                let hdr_key = rushdb_types::keys::record_hdr_key(t, *ordinal);
                let raw = txn.get(&hdr_key)?.ok_or(StorageError::CorruptLog)?;

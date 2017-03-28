//! Deterministic fault injection (documents 07/13): crashes at every
//! commit boundary and torn-tail WAL writes must converge to identical,
//! invariant-clean state after recovery.

use std::sync::Arc;

use rushdb_lmpg::import::import_document;
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use rushdb_types::keys::*;

fn state_fingerprint(store: &Store) -> u64 {
    let tenant_data = TenantId::from_ordinal(7);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for (k, v) in store
        .read_view()
        .unwrap()
        .range_prefix(&key(tenant_data, &[]))
        .unwrap()
    {
        if k.windows(7).any(|w| w == b"__log__")
            || k.starts_with(b"txn-")
            || k.windows(17).any(|w| w == b"meta/last-applied")
        {
            continue;
        }
        for b in &k {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        h = !h;
        for b in &v {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
    }
    h
}

fn raw_commit(
    store: &Store,
    tenant: TenantId,
    mutation: rushdb_types::command::Mutation,
) -> Result<rushdb_storage::CommitOutcome<()>, rushdb_storage::StorageError> {
    store.commit_for_tenant(&tenant, uuid::Uuid::new_v4(), |_ci, _txn| {
        let batch = rushdb_types::command::CommitBatch {
            command_version: rushdb_types::command::COMMAND_VERSION,
            tenant,
            transaction_id: "test-raw".into(),
            commit_key: uuid::Uuid::nil(),
            start_index: 0,
            isolation: rushdb_types::command::IsolationMode::Snapshot,
            preconditions: Vec::new(),
            mutations: vec![mutation],
        };
        Ok((batch, ()))
    })
}

#[test]
fn scrub_repair_removes_bounded_orphan_occurrences() {
    let store = Arc::new(Store::new_in_memory());
    let tenant = TenantId::from_ordinal(70);
    let _lmpg = Lmpg::new(Arc::clone(&store), tenant);
    let orphan = occ_key(tenant, rushdb_types::ids::OwnerKind::Record, 1, 2, 0);
    raw_commit(
        &store,
        tenant,
        rushdb_types::command::Mutation::PutRawKey {
            key: orphan.clone(),
            value: rushdb_types::occurrence::encode_occ_value(
                false,

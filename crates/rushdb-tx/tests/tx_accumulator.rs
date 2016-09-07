//! Storage-level coverage for the cross-service transaction accumulator:
//! read-your-writes within a tx, atomic commit, conflict detection against
//! concurrent writers (read-set and write-set), and rollback semantics.

use std::sync::Arc;

use rushdb_lmpg::expand::meta_counter_key;
use rushdb_lmpg::Lmpg;
use rushdb_storage::{Store, TxAccumulator};
use rushdb_types::command::{CommitBatch, IsolationMode, Mutation, COMMAND_VERSION};
use rushdb_types::ids::TenantId;
use uuid::Uuid;

fn tenant() -> TenantId {
    TenantId::from_ordinal(42)
}

/// Creating an Lmpg installs the deterministic expander on the store (a
/// prerequisite for `tx_apply`/`tx_commit` expansion).
fn with_expander(store: &Arc<Store>, tenant: TenantId) -> Lmpg {
    Lmpg::new(Arc::clone(store), tenant)
}

fn counter_next(txn: &mut rushdb_storage::Txn<'_>, t: TenantId, name: &str) -> u64 {
    let ckey = meta_counter_key(t, name);
    let cur = txn.get(&ckey).unwrap().map_or(0, |b| {
        u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
    });
    txn.put(ckey, (cur + 1).to_be_bytes().to_vec());
    cur + 1
}

fn create_record_batch(
    tenant: TenantId,
    txn: &mut rushdb_storage::Txn<'_>,
    record_id: [u8; 16],
    label: &str,
) -> CommitBatch {
    let ordinal = counter_next(txn, tenant, "record");
    CommitBatch {
        command_version: COMMAND_VERSION,
        tenant,
        transaction_id: String::new(),
        commit_key: Uuid::nil(),
        start_index: 0,
        isolation: IsolationMode::Snapshot,
        preconditions: vec![],
        mutations: vec![Mutation::CreateRecordHeader {
            record_id,
            ordinal,
            label: label.into(),
        }],
    }
}

#[test]
fn tx_apply_does_not_commit_and_reads_own_writes() {
    let store = Arc::new(Store::new_in_memory());
    let tenant = tenant();
    let _lmpg = with_expander(&store, tenant);
    let base_ci = store.committed_version();
    let mut tx = TxAccumulator::new(tenant, base_ci);

    // Statement 1 allocates the record counter and creates a header.
    store
        .tx_apply(&mut tx, Uuid::now_v7(), |_ci, txn| {
            Ok((create_record_batch(tenant, txn, [1u8; 16], "Doc"), ()))
        })
        .unwrap();
    assert_eq!(
        store.committed_version(),
        base_ci,
        "tx_apply must not commit"
    );

    // Statement 2 re-reads the counter: it must see statement 1's bump
    // (read-your-writes through the tx overlay).
    let seen = store
        .tx_apply(&mut tx, Uuid::now_v7(), |_ci, txn| {
            let cur = txn
                .get(&meta_counter_key(tenant, "record"))
                .unwrap()
                .map_or(0, |b| u64::from_be_bytes(b.as_slice().try_into().unwrap()));
            Ok((create_record_batch(tenant, txn, [2u8; 16], "Doc"), cur))
        })
        .unwrap();
    assert!(
        seen >= 1,
        "counter read inside tx must see its own bump, got {seen}"
    );
    assert_eq!(store.committed_version(), base_ci);


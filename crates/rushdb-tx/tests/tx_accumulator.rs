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

    // Commit publishes everything in ONE atomic commit.
    let ci = store.tx_commit(Uuid::now_v7(), &tx).unwrap();
    assert_eq!(ci, base_ci + 1);
    let snap = store.read_view().unwrap();
    assert!(
        rushdb_lmpg::reader::try_access::record_by_ord(&snap, tenant, 1)
            .unwrap()
            .is_some()
    );
    // Statement 1's expansion bumps the record counter to 2, so statement 2
    // allocates ordinal 3.
    assert!(
        rushdb_lmpg::reader::try_access::record_by_ord(&snap, tenant, 3)
            .unwrap()
            .is_some()
    );
}

#[test]
fn disjoint_tx_and_autocommit_both_commit() {
    let store = Arc::new(Store::new_in_memory());
    let tenant = tenant();
    let _lmpg = with_expander(&store, tenant);
    let base_ci = store.committed_version();

    // Concurrent autocommit writes meta key "unrelated" (disjoint from the tx).
    store
        .commit_for_tenant(&tenant, Uuid::now_v7(), |_ci, _txn| {
            use rushdb_types::command::Mutation;
            let mut batch = CommitBatch {
                command_version: COMMAND_VERSION,
                tenant,
                transaction_id: String::new(),
                commit_key: Uuid::nil(),
                start_index: 0,
                isolation: IsolationMode::Snapshot,
                preconditions: vec![],
                mutations: vec![],
            };
            batch.mutations.push(Mutation::SetMetaValue {
                key: "unrelated".into(),
                value: 1,
            });
            Ok((batch, ()))
        })
        .unwrap();

    let mut tx = TxAccumulator::new(tenant, base_ci);
    store
        .tx_apply(&mut tx, Uuid::now_v7(), |_ci, _txn| {
            use rushdb_types::command::Mutation;
            let mut batch = CommitBatch {
                command_version: COMMAND_VERSION,
                tenant,
                transaction_id: String::new(),
                commit_key: Uuid::nil(),
                start_index: 0,
                isolation: IsolationMode::Snapshot,
                preconditions: vec![],
                mutations: vec![],
            };
            batch.mutations.push(Mutation::SetMetaValue {
                key: "island".into(),
                value: 2,
            });
            Ok((batch, ()))
        })
        .unwrap();
    let ci = store.tx_commit(Uuid::now_v7(), &tx).unwrap();
    assert!(ci > base_ci);
}

#[test]
fn concurrent_counter_bump_conflicts() {
    let store = Arc::new(Store::new_in_memory());
    let tenant = tenant();
    let _lmpg = with_expander(&store, tenant);
    let base_ci = store.committed_version();

    // The tx plans a record (allocates the shared counter into its buffer).
    let mut tx = TxAccumulator::new(tenant, base_ci);
    store
        .tx_apply(&mut tx, Uuid::now_v7(), |_ci, txn| {
            Ok((create_record_batch(tenant, txn, [3u8; 16], "Doc"), ()))
        })
        .unwrap();

    // A concurrent autocommit bumps the SAME counter (same write keys).
    let lmpg = with_expander(&store, tenant);
    lmpg.create_record("Doc", None, vec![]).unwrap();

    // The tx commit must detect the write-set overlap (counter key) and abort.

//! Cross-request transaction contract (plan document 07): every outcome in
//! the set `committed / aborted / expired / unknown_retryable` is exercised,
//! commit retries are idempotent, and rollback durably records an aborted
//! outcome so a later commit never reports false success.

use std::sync::Arc;
use std::time::Duration;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::BatchOp;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_tx::{TxCoordinator, TxOutcome};
use rushdb_types::ids::TenantId;

fn setup(ttl: Duration) -> (Lmpg, TxCoordinator) {
    let store = Arc::new(Store::new_in_memory());
    let tenant = TenantId::from_ordinal(61);
    let lmpg = Lmpg::new(store.clone(), tenant);
    let coord = TxCoordinator::new(store, tenant, ttl).unwrap();
    (lmpg, coord)
}

fn create_op(label: &str, value: i64) -> BatchOp {
    BatchOp::CreateRecord {
        label: label.to_string(),
        explicit_id: None,
        fields: vec![("v".into(), FieldChange::Set(serde_json::json!(value)))],
    }
}

#[test]
fn multi_op_transaction_commits_atomically() {
    let (lmpg, coord) = setup(Duration::from_secs(60));
    let tx = coord.begin().unwrap();
    coord.stage(&tx, create_op("A", 1)).unwrap();
    coord.stage(&tx, create_op("B", 2)).unwrap();
    coord.stage(&tx, create_op("C", 3)).unwrap();

    match coord.commit(&tx) {
        TxOutcome::Committed(ci) => assert!(ci >= 1, "commit index is the batch's index"),
        other => panic!("expected committed, got {other:?}"),

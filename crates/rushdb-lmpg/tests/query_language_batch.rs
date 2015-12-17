//! Multi-operation batch tests for query-language mutation adapters:
//! `run_batch` must apply every staged op atomically and in order.

use std::sync::Arc;

use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::{BatchOp, BatchTx, FieldChange, Lmpg};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;
use uuid::Uuid;

#[test]
fn relationship_can_reference_records_created_in_the_same_atomic_batch() {
    let store = Arc::new(Store::new_in_memory());
    let graph = Lmpg::new(store, TenantId::from_ordinal(7));
    let source_id = *Uuid::now_v7().as_bytes();
    let target_id = *Uuid::now_v7().as_bytes();

    let committed = graph
        .run_batch(BatchTx {
            ops: vec![
                BatchOp::CreateRecord {
                    label: "Person".into(),
                    explicit_id: Some(source_id),
                    fields: vec![("name".into(), FieldChange::Set(json!("Alice")))],
                },
                BatchOp::CreateRecord {
                    label: "Person".into(),
                    explicit_id: Some(target_id),
                    fields: vec![("name".into(), FieldChange::Set(json!("Bob")))],
                },
                BatchOp::CreateRelationshipById {
                    rel_type: "KNOWS".into(),
                    source_id,

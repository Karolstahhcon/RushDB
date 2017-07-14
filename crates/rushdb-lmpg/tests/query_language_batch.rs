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

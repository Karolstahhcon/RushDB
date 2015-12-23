//! Fallible native reads remain pinned and reconstruct committed data.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

const TENANT: TenantId = TenantId::from_ordinal(88);

#[test]
fn pinned_read_view_preserves_committed_records_and_relationships() {
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(Arc::clone(&store), TENANT);
    let a = lmpg
        .create_record(
            "Person",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!("Ada"))),
                ("tags".into(), FieldChange::Set(json!(["x", "y"]))),
            ],
        )
        .unwrap();
    let b = lmpg.create_record("Company", None, vec![]).unwrap();
    let rel = lmpg.create_relationship("WORKS_AT", a, b, vec![]).unwrap();

    let view = store.read_view().unwrap();
    assert_eq!(
        try_access::label_members(&view, TENANT, "Person").unwrap(),
        vec![a.ordinal]
    );
    assert_eq!(
        try_access::record_by_ord(&view, TENANT, a.ordinal)
            .unwrap()

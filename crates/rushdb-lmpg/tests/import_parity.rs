//! Product-parity import (reference: records.importJson e2e specs): BFS
//! nested-object promotion to child records joined by the default relation,
//! `$vectors` extraction as data drafts, and mergeBy upsert reusing child
//! records so a new parent links to the same child (upsert-nested-linking).

use std::sync::Arc;

use rushdb_lmpg::import::{import_bfs, serialize_bfs, ImportOptions, RUSHDB_RELATION_DEFAULT};
use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::{Lmpg, RecordRef};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TenantId::from_ordinal(65))
}

fn label_count(lmpg: &Lmpg, label: &str) -> usize {
    let snap = lmpg.store.read_view().unwrap();
    try_access::label_members(&snap, lmpg.tenant, label)
        .unwrap()
        .len()
}

fn adj_rel_types(lmpg: &Lmpg, ord: u64) -> Vec<String> {
    let snap = lmpg.store.read_view().unwrap();
    try_access::adj_out(&snap, lmpg.tenant, ord, None)
        .unwrap()
        .iter()
        .map(|(t, _, _)| t.clone())
        .collect()
}

#[test]
fn bfs_nested_import_promotes_children_and_links_default_relation() {
    let lmpg = setup();
    let plan = serialize_bfs(
        &json!({
            "name": "Acme",
            "address": "1 Main St",
            "department": [
                { "name": "Eng", "headcount": 5 },
                { "name": "Sales", "headcount": 9 }
            ]
        }),
        "Company",
        &ImportOptions::default(),
    );

    // one root (Company) + two children (department)
    assert_eq!(plan.records.len(), 3);
    assert_eq!(plan.relations.len(), 2);
    assert!(plan
        .relations
        .iter()
        .all(|r| r.rel_type == RUSHDB_RELATION_DEFAULT));

    let idmap = import_bfs(&lmpg, &plan, &ImportOptions::default()).unwrap();
    assert_eq!(idmap.len(), 3);
    assert_eq!(label_count(&lmpg, "Company"), 1);
    assert_eq!(label_count(&lmpg, "department"), 2);

    // each department is linked to the company via the default relation
    let snap = lmpg.store.read_view().unwrap();
    let company_ord = try_access::label_members(&snap, lmpg.tenant, "Company").unwrap()[0];
    let rels = try_access::adj_out(&snap, lmpg.tenant, company_ord, None).unwrap();
    assert_eq!(rels.len(), 2);
    assert!(rels.iter().all(|(t, _, _)| t == RUSHDB_RELATION_DEFAULT));
    let _ = adj_rel_types(&lmpg, company_ord);
}

#[test]
fn bfs_nested_import_applies_per_key_relationship_types() {
    // Q1: `relationshipTypes` gives each nested child key its own edge type,
    // so heterogeneous promotion edges survive one nested ingest verbatim.
    let lmpg = setup();
    let options = ImportOptions::from_json(&json!({
        "relationshipTypes": {
            "department": "HAS_NEST_DEPARTMENT",
            "address": "HAS_NEST_ADDRESS"
        }
    }));
    let plan = serialize_bfs(
        &json!({
            "name": "Acme",
            "address": { "street": "1 Main St" },
            "department": [
                { "name": "Eng" },
                { "name": "Sales" }
            ]
        }),
        "Company",

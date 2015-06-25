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
        &options,
    );
    assert_eq!(plan.records.len(), 4);
    let mut types: Vec<&str> = plan.relations.iter().map(|r| r.rel_type.as_str()).collect();
    types.sort_unstable();
    assert_eq!(
        types,
        vec![
            "HAS_NEST_ADDRESS",
            "HAS_NEST_DEPARTMENT",
            "HAS_NEST_DEPARTMENT"
        ]
    );
    // Unmapped keys still fall back to the default.
    let defaulted = serialize_bfs(
        &json!({"name": "Acme", "note": {"body": "hi"}}),
        "Company",
        &options,
    );
    assert!(defaulted
        .relations
        .iter()
        .all(|r| r.rel_type == RUSHDB_RELATION_DEFAULT));

    import_bfs(&lmpg, &plan, &options).unwrap();
    let snap = lmpg.store.read_view().unwrap();
    let company_ord = try_access::label_members(&snap, lmpg.tenant, "Company").unwrap()[0];
    let mut observed = adj_rel_types(&lmpg, company_ord);
    observed.sort();
    assert_eq!(
        observed,
        vec![
            "HAS_NEST_ADDRESS",
            "HAS_NEST_DEPARTMENT",
            "HAS_NEST_DEPARTMENT"
        ]
    );
}

#[test]
fn repeated_upsert_import_does_not_duplicate_nested_relationships() {
    let lmpg = setup();
    let payload = json!({
        "name": "Acme",
        "department": [{ "name": "Engineering" }]
    });
    let options = ImportOptions {
        merge_by: Some(vec!["name".to_string()]),
        ..Default::default()
    };
    for _ in 0..2 {
        let plan = serialize_bfs(&payload, "Company", &options);
        import_bfs(&lmpg, &plan, &options).unwrap();
    }

    let snap = lmpg.store.read_view().unwrap();
    let company = try_access::label_members(&snap, lmpg.tenant, "Company").unwrap()[0];
    assert_eq!(
        try_access::adj_out(&snap, lmpg.tenant, company, Some(RUSHDB_RELATION_DEFAULT))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn upsert_nested_linking_reuses_child_and_relinks_new_parent() {
    let lmpg = setup();
    let opts = ImportOptions {
        merge_by: Some(vec![]),
        ..Default::default()
    };

    let payload_a = json!({
        "name": "Abshire - Farrell",
        "rating": 1.9,
        "department": [{ "name": "Sports", "description": "gloves" }]
    });
    let plan_a = serialize_bfs(&payload_a, "Company", &ImportOptions::default());
    import_bfs(&lmpg, &plan_a, &opts).unwrap();
    assert_eq!(label_count(&lmpg, "Company"), 1);
    assert_eq!(label_count(&lmpg, "department"), 1);

    // payload B differs only in `rating`: new parent, same child reused+relinked
    let payload_b = json!({
        "name": "Abshire - Farrell",
        "rating": 2.0,
        "department": [{ "name": "Sports", "description": "gloves" }]
    });
    let plan_b = serialize_bfs(&payload_b, "Company", &ImportOptions::default());
    let idmap_b = import_bfs(&lmpg, &plan_b, &opts).unwrap();

    assert_eq!(label_count(&lmpg, "Company"), 2, "new parent created");
    assert_eq!(
        label_count(&lmpg, "department"),
        1,
        "child reused, not duplicated"
    );

    // both companies link to the SAME department
    let snap = lmpg.store.read_view().unwrap();
    let companies = try_access::label_members(&snap, lmpg.tenant, "Company").unwrap();
    let mut linked_targets: Vec<u64> = Vec::new();
    for ord in companies {
        for (_, tgt, _) in
            try_access::adj_out(&snap, lmpg.tenant, ord, Some(RUSHDB_RELATION_DEFAULT)).unwrap()
        {
            linked_targets.push(tgt);
        }
    }
    assert_eq!(linked_targets.len(), 2);
    assert_eq!(
        linked_targets[0], linked_targets[1],
        "same child linked to both parents"
    );

    // the child draft from payload B remapped to the persisted child
    let child_draft = plan_b
        .records
        .iter()
        .find(|r| r.label == "department")
        .unwrap();
    let mapped = idmap_b
        .iter()
        .find(|m| m.draft_id == child_draft.draft_id)
        .unwrap();
    let child_snap = lmpg.store.read_view().unwrap();
    let existing = try_access::record_by_id(
        &child_snap,
        lmpg.tenant,
        &rushdb_types::ids::RecordId(mapped.persisted_id),
    )
    .unwrap();
    let child_records = try_access::label_members(&child_snap, lmpg.tenant, "department").unwrap();
    let existing = existing.unwrap();
    assert!(child_records.contains(&existing.ordinal));
    let _ = RecordRef {
        record_id: mapped.persisted_id,
        ordinal: existing.ordinal,
    };
}

#[test]
fn merge_by_keys_match_existing_within_label() {
    let lmpg = setup();
    // first import: company keyed by name
    let plan = serialize_bfs(
        &json!({ "name": "Acme", "rating": 1 }),
        "Company",
        &ImportOptions::default(),
    );
    import_bfs(&lmpg, &plan, &ImportOptions::default()).unwrap();
    assert_eq!(label_count(&lmpg, "Company"), 1);

    // re-import same name with a new field: mergeBy name -> update existing
    let opts = ImportOptions {
        merge_by: Some(vec!["name".to_string()]),
        ..Default::default()
    };
    let plan2 = serialize_bfs(
        &json!({ "name": "Acme", "rating": 5, "ceo": "Ada" }),
        "Company",
        &ImportOptions::default(),
    );
    import_bfs(&lmpg, &plan2, &opts).unwrap();
    assert_eq!(
        label_count(&lmpg, "Company"),
        1,
        "matched existing, no duplicate"
    );

    let snap = lmpg.store.read_view().unwrap();
    let ord = try_access::label_members(&snap, lmpg.tenant, "Company").unwrap()[0];
    let rec = rushdb_lmpg::reconstruct::reconstruct_record(&snap, lmpg.tenant, ord)
        .unwrap()
        .unwrap();
    assert_eq!(rec.get("ceo"), Some(&json!("Ada")), "new field appended");
}

#[test]
fn vectors_extracted_as_data_drafts() {
    let plan = serialize_bfs(
        &json!({
            "title": "ticket",
            "$vectors": [
                { "propertyName": "embedding", "vector": [0.1, 0.2], "similarityFunction": "cosine" }
            ]
        }),

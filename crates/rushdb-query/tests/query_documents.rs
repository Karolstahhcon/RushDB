//! End-to-end coverage of the documented query document
//! (docs.rushdb.com/learn/search-query/): MongoDB-style `where` operators,
//! logical composition, `$id`, label-key relationship blocks, `hops`
//! variable-length traversal, `$cycle` detection, and query-document `select`/groupBy
//! lowering.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{import, Lmpg};
use rushdb_query::document::parse_document;
use rushdb_query::{
    aggregate, aggregate_to_rows, aggregate_with_cancel, find_records, find_records_with_cancel,
    QueryError,
};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TenantId::from_ordinal(71))
}

fn run(lmpg: &Lmpg, document: &Value) -> Vec<Value> {
    let q = parse_document(document).expect("valid query document");
    if let Some(agg) = q.aggregate {
        let buckets = aggregate(lmpg, &agg).expect("aggregate");
        buckets
            .iter()
            .map(|b| Value::Object(b.keys.clone()))
            .collect()
    } else {
        find_records(lmpg, &q.search).expect("search").records
    }
}

#[test]
fn in_nin_type_ends_with_and_logical_ops() {
    let lmpg = setup();
    for (name, status, premium, free, n) in [
        ("alice", "active", true, false, 5u64),
        ("bob", "pending", false, true, 3),
        ("carol", "archived", true, true, 7),
        ("dave", "deleted", false, false, 2),
    ] {
        lmpg.create_record(
            "USER",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!(name))),
                ("status".into(), FieldChange::Set(json!(status))),
                ("premium".into(), FieldChange::Set(json!(premium))),
                ("freeAccess".into(), FieldChange::Set(json!(free))),
                ("score".into(), FieldChange::Set(json!(n))),
            ],
        )
        .unwrap();
    }

    // $in
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"status": {"$in": ["active", "pending"]}}}),
    );
    assert_eq!(rows.len(), 2);

    // $nin
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"status": {"$nin": ["deleted", "archived"]}}}),
    );
    assert_eq!(rows.len(), 2);

    // $type string + $endsWith
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {
            "$and": [
                {"name": {"$type": "string"}},
                {"name": {"$endsWith": "e"}}
            ]
        }}),
    );
    assert_eq!(rows.len(), 2); // alice, dave

    // $gt + $lte combined on one field (implicit and in operator block)
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"score": {"$gte": 3, "$lte": 5}}}),
    );
    assert_eq!(rows.len(), 2); // alice(5), bob(3)

    // $xor: exactly one of premium / freeAccess
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"$xor": [{"premium": true}, {"freeAccess": true}]}}),
    );
    assert_eq!(rows.len(), 2); // alice, bob

    // $nor: not active and not pending
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"$nor": [{"status": "active"}, {"status": "pending"}]}}),
    );
    assert_eq!(rows.len(), 2); // carol, dave

    // $not + $and nested
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"$and": [
            {"premium": true},
            {"$not": {"status": "archived"}}
        ]}}),
    );
    assert_eq!(rows.len(), 1); // alice
}

#[test]
fn datetime_component_integer_overflow_is_rejected() {
    let result = parse_document(&json!({
        "where": { "createdAt": { "$year": 18_446_744_073_709_551_615u64 } }
    }));
    match result {
        Ok(_) => panic!("overflowing component should be rejected"),
        Err(err) => assert!(err.to_string().contains("expected integer")),
    }
}

#[test]
fn id_filter_and_flat_equality() {
    let lmpg = setup();
    let r = lmpg
        .create_record(
            "DOC",
            None,
            vec![("title".into(), FieldChange::Set(json!("hello")))],
        )
        .unwrap();

    let snap = lmpg.store.read_view().unwrap();
    let json = rushdb_lmpg::reconstruct::reconstruct_record(&snap, lmpg.tenant, r.ordinal)
        .unwrap()
        .unwrap();
    let id = json["__id"].as_str().unwrap().to_string();

    let rows = run(&lmpg, &json!({"labels": ["DOC"], "where": {"$id": id}}));
    assert_eq!(rows.len(), 1);

    let dashed = uuid::Uuid::from_bytes(r.record_id).to_string();
    let rows = run(
        &lmpg,
        &json!({"labels": ["DOC"], "where": {"$id": {"$in": [dashed]}}}),
    );
    assert_eq!(rows.len(), 1);

    // flat equality: { title: "hello" }
    let rows = run(
        &lmpg,
        &json!({"labels": ["DOC"], "where": {"title": "hello"}}),
    );
    assert_eq!(rows.len(), 1);
    // missing field doesn't match
    let rows = run(&lmpg, &json!({"labels": ["DOC"], "where": {"nope": true}}));
    assert_eq!(rows.len(), 0);
}

#[test]
fn inline_aggregate_operands_feed_derived_math() {
    let lmpg = setup();
    for (name, price) in [("a", 10), ("a", 20), ("b", 8)] {
        lmpg.create_record(
            "ITEM",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!(name))),
                ("price".into(), FieldChange::Set(json!(price))),
            ],
        )
        .unwrap();
    }
    let parsed = parse_document(&json!({
        "labels": ["ITEM"],
        "select": {
            "total": {"$count": "*"},
            "ratio": {"$divide": [
                {"$sum": "$record.price"},
                {"$avg": "$record.price", "$precision": 2}
            ]}
        },
        "groupBy": ["$record.name"]
    }))
    .unwrap();
    let aggregate_request = parsed.aggregate.unwrap();
    let buckets = aggregate(&lmpg, &aggregate_request).unwrap();
    let rows = aggregate_to_rows(&lmpg, &buckets, &aggregate_request).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter().all(|row| row.get("ratio").is_some()),
        "{rows:?}"
    );
    assert!(rows.iter().all(|row| {
        row.as_object()
            .is_some_and(|obj| obj.keys().all(|key| !key.starts_with("__ratio_")))
    }));
}

#[test]
fn related_label_block_and_alias() {
    let lmpg = setup();
    let tech = import::import_document(&lmpg, "COMPANY", &json!({"name": "Tech Corp"})).unwrap();
    let eng = import::import_document(
        &lmpg,
        "DEPARTMENT",
        &json!({"name": "Engineering", "headcount": 50}),
    )
    .unwrap();
    import::import_document(
        &lmpg,
        "DEPARTMENT",
        &json!({"name": "Sales", "headcount": 10}),
    )
    .unwrap();
    lmpg.create_relationship("HAS_DEPARTMENT", tech, eng, vec![])
        .unwrap();

    // basic related block with property filter
    let rows = run(
        &lmpg,
        &json!({"labels": ["COMPANY"], "where": {
            "DEPARTMENT": {"name": "Engineering", "headcount": {"$gte": 20}}
        }}),
    );
    assert_eq!(rows.len(), 1);

    // related block with $relation direction out + no match

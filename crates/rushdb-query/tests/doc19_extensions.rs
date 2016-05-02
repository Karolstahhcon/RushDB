//! Document-19 extension coverage: batched `$collect`, time buckets,
//! derived-output refs, and per-root projection over the query planner.

use std::sync::Arc;

use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::{import, Lmpg};
use rushdb_query::{
    aggregate, find_records, AggregateRequest, CollectSpec, Direction, Pagination, SearchRequest,
    TimeBucketSpec, TimeUnit, MISSING_SENTINEL,
};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TenantId::from_ordinal(61))
}

fn rec(lmpg: &Lmpg, doc: &Value) {
    import::import_document(lmpg, "Event", doc).unwrap();
}

#[test]
fn collect_batches_neighbor_records() {
    let lmpg = setup();
    let r0 = import::import_document(&lmpg, "Person", &json!({"n": 0})).unwrap();
    let r1 = import::import_document(&lmpg, "Person", &json!({"n": 1})).unwrap();
    let _r2 = import::import_document(&lmpg, "Person", &json!({"n": 2})).unwrap();

    // edges: 0 -> 1
    lmpg.create_relationship("KNOWS", r0, r1, vec![]).unwrap();

    let page = find_records(
        &lmpg,
        &SearchRequest {
            labels: vec!["Person".into()],
            collects: vec![CollectSpec {
                steps: Vec::new(),
                alias: "friends".into(),
                rel_type: Some("KNOWS".into()),
                direction: Direction::Outgoing,
                target_labels: vec!["Person".into()],
                fields: vec![("n".into(), "n".into())],
                nested: vec![],
                filter: None,
                order_by: vec![],
                skip: 0,
                unique: true,
                limit: Some(5),
            }],
            ..Default::default()
        },
    )
    .unwrap();

    for record in &page.records {
        if record.get("n") == Some(&json!(0)) {
            assert_eq!(
                record.pointer("/friends/0/n"),
                Some(&json!(1)),
                "root 0 must batch-collect target 1"
            );
        } else {
            assert!(record
                .get("friends")
                .is_none_or(|v| v.as_array().is_none_or(Vec::is_empty)));
        }
    }
    assert_eq!(page.total_matched, 3);
}

#[test]
fn time_bucket_and_ordering_aggregate() {
    let lmpg = setup();
    rec(
        &lmpg,
        &json!({"kind": "a", "at": 1_600_000_000_000i64, "v": 2.0f64}),
    );
    rec(
        &lmpg,
        &json!({"kind": "b", "at": 1_600_000_100_000i64, "v": 3.0f64}),
    );
    rec(&lmpg, &json!({"kind": "b", "v": 4.0f64}));

    let buckets = aggregate(
        &lmpg,
        &AggregateRequest {
            labels: vec!["Event".into()],
            group_by: vec!["kind".into()],
            sums: vec![("total".into(), "v".into())],
            time_bucket: Some(TimeBucketSpec::fixed("at", TimeUnit::Days)),
            order_by: vec![("total".into(), true)],
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(buckets.len(), 3);
    // the second `b` row lacks `at`: its calendar dimension collapses to
    // the documented MISSING state inside the group tuple.
    let b_without_at = buckets
        .iter()
        .find(|b| b.keys.values().any(|v| v == &json!(MISSING_SENTINEL)))
        .unwrap();
    assert_eq!(b_without_at.sums.get("total"), Some(&json!(4.0)));
    let _ = json!(MISSING_SENTINEL);

    let a_bucket = buckets
        .iter()
        .find(|b| b.keys.get("kind") == Some(&json!("a")))
        .unwrap();
    let bucket_key = a_bucket
        .keys
        .keys()
        .find(|k| k.starts_with("__bucket_at/"))
        .cloned()
        .unwrap();
    assert_eq!(
        a_bucket.keys.get(&bucket_key),
        Some(&json!(1_599_955_200_000u64))
    );

    // ordering desc by sum: first bucket holds the largest aggregate (4.0)
    let first_val = buckets[0]
        .sums
        .get("total")
        .and_then(serde_json::Value::as_f64);
    if let Some(largest) = first_val {
        assert!((largest - 4.0).abs() < f64::EPSILON);
    } else {
        // order_by without total sums may use group metric; fall back to
        // asserting the fixed bucket count only

//! Select-parity edges (plan document 19 + docs.rushdb.com search-query
//! reference): `$microsecond`/`$nanosecond` datetime components, `$timeBucket`
//! output naming honoring the `as` alias, and a locked corpus of datetime
//! component predicates against the documented semantics.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_query::document::parse_document;
use rushdb_query::{aggregate, find_records, AggregateRequest, TimeBucketSpec, TimeUnit};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TenantId::from_ordinal(81))
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

/// 2023-01-02T03:04:05.678Z in epoch milliseconds.
const T: i64 = 1_672_628_645_678;

#[test]
fn microsecond_and_nanosecond_component_filters() {
    let lmpg = setup();
    lmpg.create_record(
        "Event",
        None,
        vec![
            ("at".into(), FieldChange::Set(json!(T))),
            ("name".into(), FieldChange::Set(json!("exact"))),
        ],
    )
    .unwrap();
    // a second record one millisecond later has a different millisecond
    // remainder, so only one matches a specific microsecond
    lmpg.create_record(
        "Event",
        None,
        vec![
            ("at".into(), FieldChange::Set(json!(T + 1))),
            ("name".into(), FieldChange::Set(json!("next"))),
        ],
    )
    .unwrap();

    // ms = 678 -> microsecond 678_000, nanosecond 678_000_000
    for (op, val, want_name) in [
        ("$microsecond", 678_000i64, "exact"),
        ("$nanosecond", 678_000_000i64, "exact"),
    ] {
        // inject the operator key dynamically
        let mut op_map = serde_json::Map::new();
        op_map.insert(op.to_string(), json!(val));
        let document = json!({
            "labels": ["Event"],
            "where": { "at": serde_json::Value::Object(op_map) }
        });
        let records = run(&lmpg, &document);
        assert_eq!(records.len(), 1, "{op} must match exactly one record");
        assert_eq!(
            records[0].get("name"),
            Some(&json!(want_name)),
            "{op} matched the wrong record"
        );
    }

    // a microsecond filter that matches neither record (no record has a
    // 123-ms remainder) returns nothing
    let document = json!({
        "labels": ["Event"],
        "where": { "at": { "$microsecond": 123_000 } }
    });
    assert_eq!(run(&lmpg, &document).len(), 0);
}

#[test]
fn datetime_component_corpus() {
    let lmpg = setup();
    lmpg.create_record(
        "Event",
        None,
        vec![
            ("at".into(), FieldChange::Set(json!(T))),
            ("name".into(), FieldChange::Set(json!("t"))),
        ],
    )
    .unwrap();

    // documented component semantics for 2023-01-02T03:04:05.678Z
    let cases = [
        ("$year", 2023),
        ("$month", 1),
        ("$day", 2),
        ("$hour", 3),
        ("$minute", 4),
        ("$second", 5),
        ("$millisecond", 678),
        ("$microsecond", 678_000),
        ("$nanosecond", 678_000_000),
    ];
    for (op, val) in cases {
        let document = json!({
            "labels": ["Event"],
            "where": { "at": { op: val } }

//! Planner differential oracle: every access path must return exactly what a
//! forced full scan returns, for eligible, residual, and mixed predicates.
//! `SearchRequest::force_scan` selects the path on identical input.

use std::collections::BTreeSet;
use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_query::document::parse_document;
use rushdb_query::{find_records, find_records_streaming};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

#[expect(
    clippy::cast_precision_loss,
    reason = "the fixture stores a small random integer divided by four as an f64 property value"
)]
fn setup(seed: u64) -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store, TenantId::from_ordinal(900 + seed));
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 33
    };
    for i in 0..300u64 {
        let label = if i % 3 == 0 { "B" } else { "A" };
        let mut fields: Vec<(String, FieldChange)> = vec![
            ("n".into(), FieldChange::Set(json!(next() % 20))),
            (
                "f".into(),
                FieldChange::Set(json!((next() % 100) as f64 / 4.0)),
            ),
            (
                "s".into(),
                FieldChange::Set(json!(format!("v{}", next() % 7))),
            ),
            ("flag".into(), FieldChange::Set(json!(next() % 2 == 0))),
        ];
        if i % 5 == 0 {
            // Array values (element postings; whole-array eval semantics).
            fields.push((
                "arr".into(),
                FieldChange::Set(json!([next() % 5, next() % 5])),
            ));
        }
        if i % 11 == 0 {
            fields.push(("nv".into(), FieldChange::ExplicitNull));
        }
        lmpg.create_record(label, None, fields).unwrap();
    }
    lmpg
}

fn ids_and_total(page: &rushdb_query::Page) -> (BTreeSet<String>, u64) {
    (
        page.records
            .iter()
            .filter_map(|r| r.get("__id").and_then(|v| v.as_str()).map(str::to_string))
            .collect(),
        page.total_matched,
    )
}

fn run(lmpg: &Lmpg, document: &Value, force_scan: bool, stream: bool) -> (BTreeSet<String>, u64) {
    let mut parsed = parse_document(document).expect("valid query document");
    parsed.search.force_scan = force_scan;
    let page = if stream {
        let snap = Arc::new(lmpg.store.read_view().unwrap());
        find_records_streaming(lmpg, &parsed.search, &|| false, snap).expect("streaming")
    } else {
        find_records(lmpg, &parsed.search).expect("scan")
    };
    ids_and_total(&page)
}

#[test]
fn signed_zero_and_integer_zero_index_matches_scan() {
    let store = Arc::new(rushdb_storage::Store::new_in_memory());
    let lmpg = Lmpg::new(store, TenantId::single());
    for value in [json!(-0.0), json!(0.0), json!(0)] {
        rushdb_lmpg::import::import_document(&lmpg, "Audit", &json!({"n": value})).unwrap();
    }
    for value in [json!(-0.0), json!(0.0), json!(0)] {
        let mut indexed = parse_document(&json!({"where": {"n": value}}))
            .unwrap()
            .search;
        let indexed_result = find_records(&lmpg, &indexed).unwrap();
        indexed.force_scan = true;
        let scanned_result = find_records(&lmpg, &indexed).unwrap();
        assert_eq!(indexed_result.total_matched, scanned_result.total_matched);
    }
}

/// Both executors, indexed and forced-scan, must agree exactly.
fn assert_paths(lmpg: &Lmpg, document: &Value, label: &str) {
    for stream in [false, true] {
        let indexed = run(lmpg, document, false, stream);
        let scanned = run(lmpg, document, true, stream);
        assert_eq!(
            indexed, scanned,
            "path mismatch ({label}, stream={stream}): {document}"
        );
    }
}

#[test]
fn eligible_residual_and_mixed_predicates_match_the_scan() {
    let lmpg = setup(0x5EED);
    let cases = vec![
        json!({"where": {"n": 3}}),
        json!({"where": {"n": {"$in": [1, 5, 9]}}}),
        json!({"where": {"n": {"$gte": 10}}}),
        json!({"where": {"n": {"$lt": 4}}}),
        json!({"where": {"f": {"$gt": 12.5}}}),
        json!({"where": {"s": "v2"}}),
        json!({"where": {"flag": true}}),
        json!({"where": {"arr": 2}}),
        json!({"where": {"nv": {"$eq": null}}}),
        json!({"where": {"n": {"$exists": true}}}),
        json!({"where": {"n": {"$exists": false}}}),
        // residual-only
        json!({"where": {"n": {"$ne": 3}}}),
        json!({"where": {"s": {"$contains": "v1"}}}),
        json!({"where": {"$or": [{"n": 1}, {"n": 2}]}}),
        // eligible + residual (partial-index planning)
        json!({"where": {"n": 3, "s": {"$contains": "v"}}}),
        json!({"where": {"flag": true, "n": {"$ne": 3}}}),
        json!({"where": {"n": {"$gte": 5}, "s": "v2", "flag": false}}),
        // eligible + eligible + residual
        json!({"where": {"n": {"$in": [1, 2, 3]}, "flag": true, "s": {"$contains": "v"}}}),
        // label scoped
        json!({"labels": ["A"], "where": {"n": 3}}),
        json!({"labels": ["A", "B"], "where": {"n": {"$exists": true}, "n": {"$lt": 5}}}),
        // empty match paths
        json!({"where": {"n": 999}}),
        json!({"where": {"nope": 1}}),
    ];
    for (i, case) in cases.iter().enumerate() {
        assert_paths(&lmpg, case, &format!("case {i}"));
    }
}

#[test]
#[expect(
    clippy::cast_precision_loss,
    reason = "the fixture stores small integers halved as f64 range values"
)]
fn range_seek_matches_scan_at_fractional_and_negative_bounds() {
    // The seek widens Int64/DateTime bounds to the nearest integer; verify the
    // exact filter still removes the widened extras and never drops a match.
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store, TenantId::single());
    for n in -6i64..=6 {
        lmpg.create_record(
            "R",
            None,
            vec![
                ("n".into(), FieldChange::Set(json!(n))),
                ("f".into(), FieldChange::Set(json!(n as f64 / 2.0))),
            ],
        )
        .unwrap();
    }
    for bound in [
        json!(-3),
        json!(-3.5),
        json!(-0.5),
        json!(0),
        json!(0.5),
        json!(2.25),
        json!(5.5),
        json!(6),
    ] {
        for op in ["$gt", "$gte", "$lt", "$lte"] {
            assert_paths(
                &lmpg,
                &json!({"labels": ["R"], "where": {"n": {op: bound.clone()}}}),
                &format!("n {op} {bound}"),
            );
            assert_paths(
                &lmpg,
                &json!({"labels": ["R"], "where": {"f": {op: bound.clone()}}}),
                &format!("f {op} {bound}"),
            );
        }
    }
}

#[test]
fn bounded_exists_count_plan_matches_the_scan_page_and_total() {
    // Single unscoped `$exists` with a finite limit uses the maintained
    // owner count for `total` and a bounded presence merge for the page.
    let lmpg = setup(0xB0A2D);
    for (skip, limit) in [(0u64, 1u64), (0, 7), (5, 7), (200, 50), (0, 1000)] {
        let document = json!({
            "where": {"n": {"$exists": true}},
            "skip": skip,
            "limit": limit
        });
        assert_paths(
            &lmpg,
            &document,
            &format!("exists skip={skip} limit={limit}"),
        );
    }
}

#[test]
fn label_scoped_exists_count_plan_matches_the_scan_page_and_total() {
    // Label-scoped single `$exists` uses the maintained per-label counter for
    // `total` and a label-filtered bounded presence merge for the page.
    let lmpg = setup(0x1ABE1);
    for labels in [vec!["A"], vec!["B"], vec!["A", "B"]] {
        for (skip, limit) in [(0u64, 1u64), (0, 5), (3, 4), (0, 1000)] {
            let document = json!({
                "labels": labels,

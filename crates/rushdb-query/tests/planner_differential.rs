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


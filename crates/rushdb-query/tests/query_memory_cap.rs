//! Per-query working-set cap (config #3): a query that would materialize more
//! than the configured budget returns `resource exhausted` instead of
//! ballooning RSS. The default (0) is off, so existing behavior is unchanged.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_query::document::parse_document;
use rushdb_query::{find_records_in_view, QueryError};
use rushdb_storage::resources::StorageResources;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn store_with_cap(cap: u64) -> Arc<Store> {
    Arc::new(
        Store::new_with_resources(StorageResources {
            query_memory_max_bytes: cap,
            ..StorageResources::default()
        })
        .unwrap(),
    )
}

fn seed(lmpg: &Lmpg) {
    for i in 0..300 {
        lmpg.create_record("Big", None, vec![("n".into(), FieldChange::Set(json!(i)))])
            .unwrap();
    }
}

#[test]
fn query_memory_cap_rejects_an_oversized_working_set() {
    let store = store_with_cap(1024);
    let lmpg = Lmpg::new(store.clone(), TenantId::single());
    seed(&lmpg);
    let parsed = parse_document(&json!({ "labels": ["Big"] })).unwrap();
    let view = Arc::new(store.read_view().unwrap());
    let err = find_records_in_view(&lmpg, &parsed.search, &|| false, view).unwrap_err();
    assert!(
        matches!(err, QueryError::ResourceLimit(_)),
        "expected a resource cap, got {err:?}"
    );
}

#[test]
fn cancellation_interrupts_candidate_discovery() {
    // The unscoped candidate scan must poll cancellation between batches, not
    // only per-candidate during predicate evaluation.
    let store = store_with_cap(0);
    let lmpg = Lmpg::new(store.clone(), TenantId::single());
    for i in 0..5000u64 {
        lmpg.create_record("Big", None, vec![("n".into(), FieldChange::Set(json!(i)))])
            .unwrap();
    }
    let view = Arc::new(store.read_view().unwrap());
    let parsed = parse_document(&json!({})).unwrap();
    let polls = std::sync::atomic::AtomicUsize::new(0);
    let cancelled = || {
        // First poll (the pre-dispatch check) passes; the next (inside the
        // candidate scan) cancels.
        polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1 >= 2
    };
    let err = find_records_in_view(&lmpg, &parsed.search, &cancelled, view).unwrap_err();
    assert!(matches!(err, QueryError::Cancelled), "{err:?}");
}

#[test]
fn query_memory_cap_is_off_by_default() {
    let store = store_with_cap(0);
    let lmpg = Lmpg::new(store.clone(), TenantId::single());
    seed(&lmpg);
    let parsed = parse_document(&json!({ "labels": ["Big"] })).unwrap();
    let view = Arc::new(store.read_view().unwrap());
    let page = find_records_in_view(&lmpg, &parsed.search, &|| false, view).unwrap();
    assert_eq!(page.total_matched, 300);
}

#[test]
fn record_collect_respects_the_configured_working_set_cap() {
    use rushdb_query::{CollectSpec, Direction};
    let store = store_with_cap(64 * 1024);
    let lmpg = Lmpg::new(store.clone(), TenantId::single());
    let root = lmpg.create_record("Root", None, vec![]).unwrap();
    for _ in 0..4 {
        let child = lmpg
            .create_record(
                "Child",
                None,
                vec![(
                    "payload".into(),
                    FieldChange::Set(json!("x".repeat(32 * 1024))),
                )],
            )
            .unwrap();
        lmpg.create_relationship("HAS", root, child, vec![])
            .unwrap();
    }
    let mut parsed = parse_document(&json!({ "labels": ["Root"] })).unwrap();
    parsed.search.collects = vec![CollectSpec {
        steps: vec![],
        alias: "children".into(),
        rel_type: Some("HAS".into()),
        direction: Direction::Outgoing,
        target_labels: vec![],
        fields: vec![],
        nested: vec![],
        filter: None,
        order_by: vec![],
        skip: 0,
        unique: true,
        limit: None,
    }];
    let view = Arc::new(store.read_view().unwrap());
    let err = find_records_in_view(&lmpg, &parsed.search, &|| false, view).unwrap_err();

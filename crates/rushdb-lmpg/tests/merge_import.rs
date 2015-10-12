//! Merge/upsert import regression coverage: `merge: true` (shorthand for
//! `mergeBy: []` + append) and explicit `mergeBy: []` must not duplicate
//! records on re-import — even with arrays, empty arrays, nulls, datetimes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rushdb_lmpg::{import, Lmpg};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn count_label(lmpg: &Lmpg, label: &str) -> u64 {
    let snap = lmpg.store.read_view().unwrap();
    rushdb_lmpg::reader::try_access::label_members(&snap, lmpg.tenant, label)
        .unwrap()
        .len() as u64
}

#[test]
fn merge_by_all_keys_does_not_duplicate() {
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store, TenantId::from_ordinal(92));

    let dataset = json!({
        "Hero": [
            {
                "id": "h1",
                "name": "Luke",
                "aliases": [],
                "tags": ["jedi", "skywalker"],
                "nickname": null,
                "score": 10,
                "active": true,
                "joined_at": 1_700_000_000_000_i64
            },
            {
                "id": "h2",
                "name": "Leia",
                "aliases": ["princess"],
                "tags": [],
                "nickname": "organa",
                "score": 20,
                "active": false,
                "joined_at": 1_600_000_000_000_i64
            }
        ]
    });

    let plain = import::ImportOptions {
        merge_by: None,
        merge_strategy: None,
        ..Default::default()
    };
    let bfs_plan = import::serialize_bfs(&dataset, "", &plain);
    import::import_bfs(&lmpg, &bfs_plan, &plain).unwrap();
    let before = count_label(&lmpg, "Hero");

    let merge = import::ImportOptions {
        merge_by: Some(vec![]),
        merge_strategy: Some("append".into()),
        ..Default::default()
    };
    let merged_plan = import::serialize_bfs(&dataset, "", &merge);
    import::import_bfs(&lmpg, &merged_plan, &merge).unwrap();
    assert_eq!(
        count_label(&lmpg, "Hero"),
        before,
        "merge by all keys must not duplicate records"
    );
}

#[test]
fn merge_true_key_is_recognized() {
    // `options: { merge: true }` must request upsert-by-all-keys instead of
    // being silently ignored (which created duplicates on re-import).
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store, TenantId::from_ordinal(93));
    let dataset = json!({
        "Hero": [{ "id": "h1", "name": "Luke", "score": 10 }]
    });
    let plain = import::ImportOptions {
        merge_by: None,
        merge_strategy: None,
        ..Default::default()
    };
    let bfs_plan = import::serialize_bfs(&dataset, "", &plain);
    import::import_bfs(&lmpg, &bfs_plan, &plain).unwrap();
    let before = count_label(&lmpg, "Hero");

    let opts = import::ImportOptions::from_json(&json!({ "merge": true }));
    assert!(opts.upsert_requested(), "merge:true must request upsert");
    assert_eq!(opts.merge_by, Some(vec![]));
    assert_eq!(opts.merge_strategy, Some("append".into()));
    let merged_plan = import::serialize_bfs(&dataset, "", &opts);
    import::import_bfs(&lmpg, &merged_plan, &opts).unwrap();
    assert_eq!(
        count_label(&lmpg, "Hero"),
        before,
        "merge:true must not duplicate"
    );
}

#[test]
fn merge_lookup_is_index_backed_not_a_label_scan() {
    // Regression guard: matching every merge key by reconstructing every record
    // in the label made upserts O(rows * label_size). A 192-row merge took ~1
    // minute on a production store because the label held thousands of records.
    // The lookup must instead use the equality postings. We self-calibrate the
    // budget against the batched plain insert (one commit, O(n)) so the bound
    // tracks machine speed instead of an absolute constant.
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store, TenantId::from_ordinal(94));

    let n = 2_000usize;
    let rows: Vec<serde_json::Value> = (0..n)
        .map(|i| json!({ "uid": format!("u{i}"), "rank": i }))
        .collect();
    let dataset = json!({ "Widget": rows });

    let plain = import::ImportOptions::default();
    let started = Instant::now();
    import::import_bfs(&lmpg, &import::serialize_bfs(&dataset, "", &plain), &plain).unwrap();
    let baseline = started.elapsed();

    let merge = import::ImportOptions {
        merge_by: Some(vec![]),
        merge_strategy: Some("append".into()),
        ..Default::default()
    };
    let started = Instant::now();
    import::import_bfs(&lmpg, &import::serialize_bfs(&dataset, "", &merge), &merge).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(count_label(&lmpg, "Widget"), n as u64, "no duplicates");
    let budget = baseline * 10 + Duration::from_secs(5);
    assert!(
        elapsed < budget,
        "merge took {elapsed:?} (budget {budget:?}); matching likely regressed to a label scan"
    );
}

#[test]
fn merge_duplicate_keys_within_one_import_upsert_once() {
    // Two rows with the same merge key in one payload must resolve to a single
    // record: the first creates it, the second upserts the row the first
    // created. The batched path cannot see its own uncommitted writes, so this
    // chunk must fall back to per-record commits.
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store, TenantId::from_ordinal(95));
    let dataset = json!({
        "Hero": [
            { "id": "h1", "name": "Luke", "score": 1 },
            { "id": "h1", "name": "Luke", "score": 2 },
            { "id": "h2", "name": "Leia", "score": 3 }
        ]
    });
    let opts = import::ImportOptions {
        merge_by: Some(vec!["id".to_string()]),
        merge_strategy: Some("append".into()),

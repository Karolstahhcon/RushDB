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

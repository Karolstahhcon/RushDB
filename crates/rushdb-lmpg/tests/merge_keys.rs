//! MERGE-1 (b): which keys an upsert may match on. `__id` is the record
//! identity (looked up by id, never stored as a property), every other name
//! starting with `__` is reserved for the engine, `_id` is an ordinary
//! property, and a key without an index whose population is over the
//! unindexed-walk limit is refused before the walk with the index to
//! declare. With no limit (the default, QRY-3) the walked values count
//! against `query-memory-max`.

use std::sync::Arc;

use rushdb_lmpg::{import, Lmpg};
use rushdb_storage::resources::StorageResources;
use rushdb_storage::Store;
use rushdb_types::ids::{OwnerKind, TenantId};
use serde_json::{json, Value};

fn engine(walk_limit: u64) -> Lmpg {
    let store = Store::new_with_resources(StorageResources {
        unindexed_walk_max_owners: walk_limit,
        ..StorageResources::default()
    })
    .unwrap();
    Lmpg::new(Arc::new(store), TenantId::single())
}

fn upsert(lmpg: &Lmpg, data: &Value, merge_by: &[&str]) -> Result<(), String> {
    let options = import::ImportOptions {
        merge_by: Some(merge_by.iter().map(|key| (*key).to_owned()).collect()),
        merge_strategy: Some("append".into()),
        ..Default::default()
    };
    import::import_bfs(lmpg, &import::serialize_bfs(data, "", &options), &options)
        .map(|_| ())
        .map_err(|error| error.to_reason())
}

fn records(lmpg: &Lmpg, label: &str) -> Vec<Value> {
    let snap = lmpg.store.read_view().unwrap();
    rushdb_lmpg::reader::try_access::label_members(&snap, lmpg.tenant, label)
        .unwrap()
        .into_iter()
        .map(|ord| {
            rushdb_lmpg::reconstruct::reconstruct_record(&snap, lmpg.tenant, ord)
                .unwrap()
                .unwrap()
        })
        .collect()
}

fn has_property(lmpg: &Lmpg, name: &str) -> bool {
    let snap = lmpg.store.read_view().unwrap();
    let key = rushdb_lmpg::reader::keys_ext::family_sig_key(
        lmpg.tenant,
        "default",
        OwnerKind::Record,
        name,
    );
    snap.get(&key).unwrap().is_some()
}

#[test]
fn an_upsert_by_id_finds_the_record_by_id_and_never_stores_an_id_property() {
    let lmpg = engine(0);
    let id = "0192f1f0c0de7000a000000000000001";
    upsert(
        &lmpg,
        &json!({ "Hero": [{ "__id": id, "name": "Luke" }] }),
        &["__id"],
    )
    .unwrap();
    let created = records(&lmpg, "Hero");
    assert_eq!(created.len(), 1, "a miss creates the record");
    assert_eq!(created[0]["__id"], id, "with the given id");

    let dashed = "0192f1f0-c0de-7000-a000-000000000001";
    upsert(
        &lmpg,
        &json!({ "Hero": [{ "__id": dashed, "name": "Han" }] }),
        &["__id"],
    )
    .unwrap();
    let updated = records(&lmpg, "Hero");
    assert_eq!(updated.len(), 1, "a hit updates in place: {updated:?}");
    assert_eq!(updated[0]["name"], "Han");
    assert!(
        !has_property(&lmpg, "__id"),
        "`__id` is the identity, not a property"
    );

    // Every incoming key (merge: true) includes the id; a different value of
    // another key means no match, so the create collides with the id.
    let error = upsert(
        &lmpg,
        &json!({ "Hero": [{ "__id": id, "name": "Leia" }] }),
        &[],
    )
    .unwrap_err();

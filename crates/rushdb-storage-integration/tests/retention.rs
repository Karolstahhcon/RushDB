//! 3A — protected read-view retention and explicit expiry. A pinned historical
//! read must hold the history floor through GC; dropping the handle releases
//! the pin; expired (leaked) pins are reclaimed; reads at reclaimed CIs fail.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn setup() -> (Lmpg, Arc<Store>) {
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store.clone(), TenantId::from_ordinal(7));
    (lmpg, store)
}

fn current_value(lmpg: &Lmpg, view: &rushdb_storage::ReadView, ordinal: u64) -> i64 {
    let rebuilt = rushdb_lmpg::reconstruct::reconstruct_record(view, lmpg.tenant, ordinal)
        .unwrap()
        .expect("record exists");
    rebuilt
        .get("v")
        .and_then(serde_json::Value::as_i64)
        .unwrap()
}

#[test]
fn read_pin_protects_history_through_gc_then_releases() {
    let (lmpg, store) = setup();
    let rec = lmpg
        .create_record("Doc", None, vec![("v".into(), FieldChange::Set(json!(0)))])
        .unwrap();
    let pinned_ci = store.committed_version();

    // Pin a historical read at this commit.
    let pinned_view = store.read_at(pinned_ci).unwrap();
    assert_eq!(pinned_view.commit_index(), pinned_ci);
    assert!(store.retention_summary().1 >= 1, "pin must be recorded");

    // Advance the record far past the pinned CI, then GC up to the head.
    for i in 1..=50 {
        lmpg.update_fields(
            rushdb_lmpg::RecordRef {
                ordinal: rec.ordinal,
                record_id: rec.record_id,
            },
            vec![("v".into(), FieldChange::Set(json!(i)))],
        )
        .unwrap();
    }
    let head = store.committed_version();
    assert!(head > pinned_ci);
    store.retire_before(head).unwrap();

    // GC could not retire below the oldest active pin.
    let (oldest, _) = store.retention_summary();
    assert_eq!(oldest, Some(pinned_ci), "pin must hold the history floor");

    // The pinned historical read still sees the OLD value (v=0).
    assert_eq!(current_value(&lmpg, &pinned_view, rec.ordinal), 0);

    // Release the handle: the pin is gone, so a second GC reclaims the history
    // below the head and the old CI is no longer readable.
    drop(pinned_view);
    assert_eq!(store.retention_summary().1, 0, "pin released on drop");
    store.retire_before(head).unwrap();
    let err = store.read_at(pinned_ci);

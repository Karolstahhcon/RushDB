//! MVCC history garbage collection (plan document 07 retention): superseded
//! versions at or before the GC watermark are reclaimed before redb
//! compaction. Current data and snapshot reads at/after the
//! watermark stay correct; reads before the watermark are reclaimed.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TenantId::from_ordinal(91))
}

#[test]
fn gc_reclaims_superseded_versions_keeps_current_state() {
    let lmpg = setup();
    let store = lmpg.store.clone();

    let rec = lmpg
        .create_record("Doc", None, vec![("v".into(), FieldChange::Set(json!(0)))])
        .unwrap();
    let before_updates = store.committed_version();

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
    let after_updates = store.committed_version();
    assert!(after_updates - before_updates >= 50);

    // current state is v=50
    let snap = lmpg.store.read_view().unwrap();
    let rebuilt = rushdb_lmpg::reconstruct::reconstruct_record(&snap, lmpg.tenant, rec.ordinal)
        .unwrap()
        .unwrap();
    assert_eq!(rebuilt.get("v"), Some(&json!(50)));

    // time-travel read just before GC sees an old version
    let key = rushdb_types::keys::record_id_key(lmpg.tenant, &rec.record_id);
    // after many updates, reading at a mid watermark returns the version at
    // that point (versions are preserved before GC)
    let mid = before_updates + 10;
    let mid_val = store.read_at(mid).unwrap().get(&key).unwrap();
    assert!(
        mid_val.is_some(),
        "historical header must remain readable before GC"
    );

    // GC at the current commit: every superseded version <= after_updates is
    // reclaimable (the snapshot view is materialized in memory/checkpoint).
    let watermark = after_updates;
    let reclaimed = store.gc(watermark).unwrap();
    assert!(
        reclaimed > 0,
        "50 updates must produce reclaimable version entries"
    );

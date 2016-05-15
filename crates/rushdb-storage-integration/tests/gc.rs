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

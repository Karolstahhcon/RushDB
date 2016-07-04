//! Serializable isolation and MVCC snapshot coverage for the storage commit
//! path: snapshot time travel, range-overlap conflicts, and disjoint commits.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{reader, Lmpg};
use rushdb_storage::{CommitOutcome, DependencySet, Store};
use rushdb_types::command::{CommitBatch, IsolationMode, Mutation, COMMAND_VERSION};
use rushdb_types::ids::TenantId;

use serde_json::json;
use uuid::Uuid;

fn tenant() -> TenantId {
    TenantId::from_ordinal(42)
}

#[test]
fn snapshot_time_travel_reads_older_state() {
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store, tenant());

    let r = lmpg
        .create_record(
            "Doc",
            None,
            vec![("v".into(), FieldChange::Set(json!("v1")))],
        )
        .unwrap();

    let ci_after_v1 = lmpg.store.committed_version();

    lmpg.update_fields(r, vec![("v".into(), FieldChange::Set(json!("v2")))])
        .unwrap();
    let ci_after_v2 = lmpg.store.committed_version();
    assert!(ci_after_v2 > ci_after_v1);

    // Read views pinned at each commit see exactly their own state.
    let old_view = lmpg.store.read_at(ci_after_v1).unwrap();
    assert_eq!(
        reader::try_access::record_by_ord(&old_view, tenant(), r.ordinal)
            .unwrap()
            .map(|_| ()),
        Some(())
    );
    let old_json = rushdb_lmpg::reconstruct::reconstruct_record(&old_view, tenant(), r.ordinal)
        .unwrap()
        .unwrap();
    assert_eq!(old_json.get("v"), Some(&json!("v1")));

    let new_json = rushdb_lmpg::reconstruct::reconstruct_record(
        &lmpg.store.read_view().unwrap(),
        tenant(),
        r.ordinal,
    )
    .unwrap()
    .unwrap();
    assert_eq!(new_json.get("v"), Some(&json!("v2")));

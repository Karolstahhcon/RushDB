//! P07 duplicate values + bounded reconstruction.
//!
//! - Two records holding the same value must both be found, and the value
//!   dictionary must count both; updating/deleting one must leave the other.
//! - `reconstruct::reconstruct_record_limited` must reject a record whose
//!   live occurrence count exceeds a bound, and succeed below it.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::reader::try_access as access;
use rushdb_lmpg::{BatchOp, Lmpg, RecordRef};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

const TENANT: TenantId = TenantId::from_ordinal(74);

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TENANT)
}

/// The property id for a field on the label, via the family signature.
fn property_id(lmpg: &Lmpg, _label: &str, field: &str) -> u64 {
    use rushdb_types::ids::OwnerKind;
    use rushdb_types::keys::*;
    let norm = rushdb_types::value::normalize_name(field);
    let sig = key(
        lmpg.tenant,
        &[
            P_FAMILY_SIG,
            b"default",
            &[OwnerKind::Record.as_byte()],
            norm.as_bytes(),
        ],
    );
    let raw = lmpg.store.try_get(&sig).unwrap().unwrap();
    u64::from_be_bytes(raw[..8].try_into().unwrap())
}

/// Owners whose `v` currently holds `value`, read through the label-scoped
/// presence rows: presence gives the candidates, the occurrence row
/// confirms the value, and the per-property value dictionary count
/// must agree with the confirmed set.
fn owners_with_value(lmpg: &Lmpg, pid: u64, value: i64) -> Vec<u64> {
    use rushdb_types::ids::OwnerKind;
    let view = lmpg.store.read_view().unwrap();
    let candidates = rushdb_lmpg::derived::presence_owners(
        &view,
        TENANT,
        OwnerKind::Record,
        "D",
        pid,
        rushdb_types::keys::NULLNESS_VALUE,
    )
    .unwrap();
    let mut owners = Vec::new();
    for owner in candidates {
        for occ in access::occurrences_of_owner(&view, TENANT, OwnerKind::Record, owner).unwrap() {
            if occ.pid == pid
                && rushdb_lmpg::reconstruct::decode_value(occ.value, &occ.payload).unwrap()
                    == json!(value)

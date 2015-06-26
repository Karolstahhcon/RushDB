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
            {
                owners.push(owner);
            }
        }
    }
    let ty = rushdb_types::model::LogicalType::Int64;
    let mut entry = vec![rushdb_types::encoding::logical_tag(ty)];
    entry.extend(
        rushdb_types::encoding::encode_order_preserving(
            ty,
            &rushdb_types::value::Scalar::Int64(value),
        )
        .unwrap(),
    );
    assert_eq!(
        rushdb_lmpg::derived::value_count(&view, TENANT, OwnerKind::Record, "D", pid, &entry)
            .unwrap(),
        owners.len() as u64,
        "dictionary count agrees with the owners holding the value"
    );
    owners
}

#[test]
fn duplicate_values_keep_all_owners_and_delete_removes_one() {
    let lmpg = setup();
    let a = lmpg
        .create_record("D", None, vec![("v".into(), FieldChange::Set(json!(7)))])
        .unwrap();
    let b = lmpg
        .create_record("D", None, vec![("v".into(), FieldChange::Set(json!(7)))])
        .unwrap();
    let pid = property_id(&lmpg, "D", "v");

    // Both owners must be found for value 7.
    let set_a = owners_with_value(&lmpg, pid, 7);
    assert!(
        set_a.contains(&a.ordinal) && set_a.contains(&b.ordinal),
        "duplicate value must keep BOTH owners, got {set_a:?}"
    );

    // Update A away from value 7; B must remain the sole owner.
    lmpg.update_fields(
        RecordRef {
            ordinal: a.ordinal,
            record_id: a.record_id,
        },
        vec![("v".into(), FieldChange::Set(json!(8)))],
    )
    .unwrap();
    let set = owners_with_value(&lmpg, pid, 7);
    assert!(
        set.contains(&b.ordinal) && !set.contains(&a.ordinal),
        "after A moves away, only B remains, got {set:?}"
    );

    // Delete B; no owner holds 7 and its dictionary entry is gone.
    let snap = lmpg.store.read_view().unwrap();
    let b_hdr = access::record_by_ord(&snap, TENANT, b.ordinal)
        .unwrap()
        .unwrap();
    lmpg.delete_record(RecordRef {
        ordinal: b_hdr.ordinal,
        record_id: b_hdr.record_id,

//! Vertical-slice integration tests for embedding spaces, bindings,
//! candidate scoping, top-K ranking, and owner projection (items 7-10).

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::{reader::TryRead, Lmpg};
use rushdb_storage::Store;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::*;
use rushdb_types::occurrence::BindingStatus;
use rushdb_vector::{SourceRef, VectorEngine, VectorError};
use serde_json::json;

/// Creates an independent engine over one in-memory store; returns the
/// graph facade plus a second shared-store facade for the vector
/// projection layer (the same runtime composition the platform uses).
fn setup() -> (Lmpg, VectorEngine) {
    let store = Arc::new(Store::new_in_memory());
    let tenant = TenantId::from_ordinal(11);
    let lmpg = Lmpg::new(store.clone(), tenant);
    let vecs = VectorEngine::new(Arc::new(Lmpg::new(store, tenant)));
    (lmpg, vecs)
}

fn pid_of(lmpg: &Lmpg, scope: OwnerKind, name: &str) -> u64 {
    let norm = rushdb_types::value::normalize_name(name);
    let sig = key(
        lmpg.tenant,
        &[
            P_FAMILY_SIG,
            b"default",
            &[scope.as_byte()],
            norm.as_bytes(),
        ],
    );
    let raw = lmpg
        .store
        .get(&sig)
        .unwrap()
        .expect("family signature exists");
    u64::from_be_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ])
}

/// Item 7: two immutable Spaces bind one Record-owned text occurrence; two
/// more Spaces bind one Relationship-owned occurrence.
#[test]
fn item7_two_spaces_share_one_source() {
    let (lmpg, vecs) = setup();
    let rec = lmpg
        .create_record(
            "Doc",
            None,
            vec![("text".into(), FieldChange::Set(json!("alpha beta")))],
        )
        .unwrap();

    let s1 = vecs.ensure_space("title-v1", 4).unwrap();
    let s2 = vecs.ensure_space("fulltext-v1", 4).unwrap();
    assert_ne!(s1.space_id, s2.space_id);

    let src = SourceRef {
        kind: OwnerKind::Record,
        owner_ord: rec.ordinal,
        pid: pid_of(&lmpg, OwnerKind::Record, "text"),
        elem: 0,
    };
    let slot_a = vecs.bind("title-v1", src, &[1.0, 0.0, 0.0, 0.0]).unwrap();
    let slot_b = vecs
        .bind("fulltext-v1", src, &[0.0, 1.0, 0.0, 0.0])
        .unwrap();
    let _ = (slot_a, slot_b); // slots are per-Space; both spaces hold their copy
    assert_eq!(
        vecs.bindings_of(s1.space_id).unwrap().len(),
        1,
        "each Space keeps its own binding"
    );

    // Immutable contract enforcement on dims/model change.
    match vecs.ensure_space("title-v1", 8) {
        Err(VectorError::Conflict(msg)) => assert!(msg.contains("immutable space mismatch")),
        other => panic!("expected immutability error, got {other:?}"),
    }

    // Relationship-owned occurrence bound into two spaces.
    let target = lmpg.create_record("Doc", None, vec![]).unwrap();
    let rel = lmpg
        .create_relationship(
            "MENTIONS",
            rec,
            target,
            vec![("label".into(), FieldChange::Set(json!("x")))],
        )
        .unwrap();
    let rsrc = SourceRef {
        kind: OwnerKind::Relationship,
        owner_ord: rel.ordinal,
        pid: pid_of(&lmpg, OwnerKind::Relationship, "label"),
        elem: 0,
    };
    let ra = vecs.ensure_space("rel-a", 4).unwrap();
    let rb = vecs.ensure_space("rel-b", 4).unwrap();
    assert_ne!(ra.space_id, rb.space_id);
    vecs.bind("rel-a", rsrc, &[1.0, 1.0, 0.0, 0.0]).unwrap();
    vecs.bind("rel-b", rsrc, &[0.0, 0.0, 1.0, 0.0]).unwrap();
}

/// Item 10: source value update excludes old bindings as stale through
/// hash mismatch, after explicit revalidation (projector lag model).
#[test]
fn item10_stale_exclusion_after_source_update() {
    let (lmpg, vecs) = setup();
    let rec = lmpg
        .create_record(
            "Doc",
            None,
            vec![("text".into(), FieldChange::Set(json!("original value")))],
        )
        .unwrap();
    vecs.ensure_space("doc-space", 3).unwrap();
    let src = SourceRef {
        kind: OwnerKind::Record,
        owner_ord: rec.ordinal,
        pid: pid_of(&lmpg, OwnerKind::Record, "text"),

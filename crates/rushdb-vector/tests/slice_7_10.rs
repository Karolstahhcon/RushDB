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

//! Similarity metric coverage: cosine / dot / euclidean scoring and ranking.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{Lmpg, RecordRef};
use rushdb_storage::Store;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{key, P_FAMILY_SIG};
use rushdb_types::occurrence::Similarity;
use rushdb_vector::{SourceRef, VectorEngine};

fn setup() -> (Lmpg, VectorEngine) {
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store.clone(), TenantId::from_ordinal(91));
    let vecs = VectorEngine::new(Arc::new(Lmpg::new(store, TenantId::from_ordinal(91))));
    (lmpg, vecs)
}

fn bind_one(
    lmpg: &Lmpg,
    vecs: &VectorEngine,
    alias: &str,
    label: &str,
    field: &str,
    vector: &[f32],
) -> SourceRef {
    let r: RecordRef = lmpg
        .create_record(
            label,
            None,
            vec![(field.into(), FieldChange::Set(serde_json::json!("v")))],
        )
        .unwrap();
    let pref = key(
        lmpg.tenant,
        &[
            P_FAMILY_SIG,
            b"default",
            &[OwnerKind::Record.as_byte()],
            field.as_bytes(),
        ],
    );
    let raw = lmpg.store.read_view().unwrap().get(&pref).unwrap().unwrap();
    let pid = u64::from_be_bytes(raw.as_slice().try_into().unwrap());
    let src = SourceRef {
        kind: OwnerKind::Record,
        owner_ord: r.ordinal,
        pid,
        elem: 0,
    };
    vecs.bind(alias, src, vector).unwrap();
    src
}

#[test]
fn cosine_dot_euclidean_scores_and_ranking() {
    let (lmpg, vecs) = setup();

    // Three collinear-ish vectors: A and B are close, C is far.
    let a = [1.0f32, 0.0];
    let b = [0.95, 0.31];
    let c = [-1.0, 0.0];

    vecs.ensure_space_similarity("cos", 2, Similarity::Cosine)
        .unwrap();
    vecs.ensure_space_similarity("dot", 2, Similarity::Dot)
        .unwrap();
    vecs.ensure_space_similarity("euc", 2, Similarity::Euclidean)
        .unwrap();
    let _sa = bind_one(&lmpg, &vecs, "cos", "Doc", "v", &a);
    let _sb = bind_one(&lmpg, &vecs, "cos", "Doc", "v", &b);
    let _sc = bind_one(&lmpg, &vecs, "cos", "Doc", "v", &c);
    bind_one(&lmpg, &vecs, "dot", "Doc", "v", &a);
    bind_one(&lmpg, &vecs, "dot", "Doc", "v", &b);
    bind_one(&lmpg, &vecs, "dot", "Doc", "v", &c);
    bind_one(&lmpg, &vecs, "euc", "Doc", "v", &a);
    bind_one(&lmpg, &vecs, "euc", "Doc", "v", &b);
    bind_one(&lmpg, &vecs, "euc", "Doc", "v", &c);

    // Exact self-query score per metric.

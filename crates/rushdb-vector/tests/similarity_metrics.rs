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

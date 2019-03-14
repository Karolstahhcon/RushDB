//! ANN quality gate (document 09/13): recall of the pivot projection must
//! match exhaustive exact search on clustered synthetic data, filters must
//! shrink results, and corrupt generations must degrade to exact scans.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{reader::TryRead, Lmpg, RecordRef};
use rushdb_storage::Store;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{key, P_FAMILY_SIG};
use rushdb_vector::{SourceRef, VectorEngine};

#[expect(
    clippy::cast_precision_loss,
    reason = "test fixture jitter is derived from a small grid index and only needs approximate f32 resolution"
)]
fn seedable(i: u64) -> (f32, f32) {
    // two well-separated gaussian-ish clusters on a deterministic grid
    let cluster = i % 2;
    let jitter = ((i * 37) % 100) as f32 / 100.0 - 0.5;
    if cluster == 0 {
        (0.9 + jitter * 0.05, 0.1 + jitter * 0.05)
    } else {
        (0.1 + jitter * 0.05, 0.9 + jitter * 0.05)
    }
}

fn setup(n: u64) -> (Lmpg, VectorEngine, Vec<SourceRef>) {
    setup_with(n, |i| {
        let (x, y) = seedable(i);
        [x, y]
    })
}

fn setup_with(n: u64, coords: impl Fn(u64) -> [f32; 2]) -> (Lmpg, VectorEngine, Vec<SourceRef>) {
    setup_on(Arc::new(Store::new_in_memory()), n, coords)
}

fn setup_on(
    store: Arc<Store>,
    n: u64,
    coords: impl Fn(u64) -> [f32; 2],
) -> (Lmpg, VectorEngine, Vec<SourceRef>) {
    let lmpg = Lmpg::new(store.clone(), TenantId::from_ordinal(81));
    let vecs = VectorEngine::new(Arc::new(Lmpg::new(store, TenantId::from_ordinal(81))));
    vecs.ensure_space("clustered", 2).unwrap();

    let mut sources = Vec::new();
    for i in 0..n {
        let r: RecordRef = lmpg
            .create_record(
                "V",
                None,
                vec![("seed".into(), FieldChange::Set(serde_json::json!(i)))],
            )
            .unwrap();
        // find the family id for field `seed`
        let norm = rushdb_types::value::normalize_name("seed");
        let sig = key(
            lmpg.tenant,
            &[
                P_FAMILY_SIG,
                b"default",
                &[OwnerKind::Record.as_byte()],
                norm.as_bytes(),
            ],
        );
        let raw = lmpg.store.get(&sig).unwrap().expect("family signature");
        let pid = u64::from_be_bytes(raw[..8].try_into().unwrap());

        let src = SourceRef {
            kind: OwnerKind::Record,
            owner_ord: r.ordinal,
            pid,
            elem: 0,
        };
        let vector = coords(i);
        vecs.bind("clustered", src, &vector).unwrap();
        sources.push(src);
    }
    (lmpg, vecs, sources)
}

#[test]
fn ann_search_answers_exactly_when_the_logical_cache_is_disabled() {
    let store = Store::new_with_resources(rushdb_storage::resources::StorageResources {
        logical_cache_bytes: 0,
        ..Default::default()
    })
    .unwrap();
    let (_lmpg, vecs, _sources) = setup_on(Arc::new(store), 32, |i| {
        let (x, y) = seedable(i);
        [x, y]
    });
    let query = [0.92_f32, 0.08];
    let exact = vecs.search_exact("clustered", &query, 5, None).unwrap();
    let approx = vecs
        .search_ann("clustered", &query, 5, 24, None)
        .expect("no ANN may be resident, so the search falls back to exact");
    let ords = |hits: &[rushdb_vector::ScoredSource]| {
        hits.iter().map(|h| h.source.owner_ord).collect::<Vec<_>>()
    };
    assert_eq!(ords(&approx), ords(&exact));
}

#[test]
fn ann_recall_matches_exact_on_clustered_data() {

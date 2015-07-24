//! Filtered-ANN benchmark (P15/vector-search planning evidence).
//!
//! The engine's gather-then-filter ANN path protects recall via a degradation
//! contract: if fewer than `k.min(ef)` gathered candidates survive the filter,
//! it falls back to the bounded exact filtered scan. That means recall stays
//! high but the filter can *defeat ANN efficiency* by silently converting
//! approximate searches into exact scans. This benchmark measures:
//!
//! - recall@10 of filtered ANN vs the exact filtered ground truth,
//! - latency of filtered ANN vs exact (does the filter negate the ANN win?),
//! - the inferred exact-fallback rate (ANN result identical to exact result),
//! - under correlated (spatially-clustered) and uncorrelated (uniform) filters,
//!   across selectivity and oversampling (ef).

use std::sync::Arc;
use std::time::Instant;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{reader::TryRead, Lmpg, RecordRef};
use rushdb_storage::Store;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{key, P_FAMILY_SIG};
use rushdb_vector::{SourceRef, VectorEngine};

const DIMS: usize = 64;
const N_CATEGORIES: usize = 20;

#[expect(
    clippy::cast_precision_loss,
    reason = "synthetic benchmark generator deliberately maps a 31-bit integer state into [-0.5, 0.5) and only needs f32 resolution"
)]
fn make_rng(seed: u64) -> impl FnMut() -> f32 {
    let mut state = seed;
    move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((state >> 33) as f32 / (1u64 << 31) as f32) - 0.5
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "ordinals are reduced modulo a small constant, so the result always fits a 32-bit usize"
)]
fn category_of_ord(ord: u64) -> usize {
    (ord as usize) % N_CATEGORIES
}

struct Fixture {
    vecs: VectorEngine,
    n_categories: usize,
}

impl Fixture {
    fn build(n: usize, correlated: bool, seed: u64) -> Fixture {
        let store = Arc::new(Store::new_in_memory());
        let lmpg = Lmpg::new(store.clone(), TenantId::from_ordinal(71));
        let vecs = VectorEngine::new(Arc::new(Lmpg::new(store, TenantId::from_ordinal(71))));
        vecs.ensure_space("bench", u32::try_from(DIMS).unwrap())
            .unwrap();

        let mut centroids: Vec<Vec<f32>> = Vec::new();
        let mut rng = make_rng(seed);
        for _c in 0..N_CATEGORIES {
            let dir: Vec<f32> = (0..DIMS).map(|_| rng()).collect();
            let norm: f32 = dir.iter().map(|v| v * v).sum::<f32>().sqrt();
            centroids.push(dir.iter().map(|v| v / norm).collect::<Vec<_>>());
        }

        let mut rng = make_rng(seed ^ 0xABCD);
        for i in 0..n as u64 {
            let r: RecordRef = lmpg
                .create_record(
                    "V",
                    None,
                    vec![("seed".into(), FieldChange::Set(serde_json::json!(i)))],
                )
                .unwrap();
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
            let cat = category_of_ord(r.ordinal);
            let vector: Vec<f32> = if correlated {
                let mut v: Vec<f32> = (0..DIMS).map(|_| rng() * 0.08).collect();
                for d in 0..DIMS {
                    v[d] += centroids[cat][d];
                }
                v
            } else {
                (0..DIMS).map(|_| rng() * 2.0 - 1.0).collect()
            };
            vecs.bind("bench", src, &vector).unwrap();
        }
        Fixture {
            vecs,
            n_categories: N_CATEGORIES,
        }
    }

    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "selectivity is a fraction in [0,1] over a small category count, so the ceiling fits usize exactly"
    )]
    fn allowed_for_selectivity(&self, selectivity: f64) -> std::collections::HashSet<usize> {
        let count = ((self.n_categories as f64) * selectivity).ceil() as usize;
        (0..count.min(self.n_categories)).collect()
    }

    /// Queries uniformly over the space (not anchored to any allowed centroid),
    /// so the true nearest neighbours span categories and the filter removes a
    /// real fraction of them. This is the hard, realistic case.
    fn queries(count: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = make_rng(seed ^ 0x1357);
        (0..count)
            .map(|_| (0..DIMS).map(|_| rng() * 2.0 - 1.0).collect())
            .collect()
    }

    fn allow_categories(
        allowed: &std::collections::HashSet<usize>,
    ) -> impl Fn(SourceRef) -> bool + '_ {
        move |src| allowed.contains(&category_of_ord(src.owner_ord))
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "benchmark latency is reported per query and only needs f64 precision for printing"
)]
fn run_exact(
    vecs: &VectorEngine,
    queries: &[Vec<f32>],
    k: usize,
    allowed: &std::collections::HashSet<usize>,
) -> (Vec<Vec<u64>>, f64) {
    let snap = vecs.snapshot().unwrap();
    let space = vecs.lookup_space_in_view(&snap, "bench").unwrap().unwrap();
    let candidate = Fixture::allow_categories(allowed);
    let mut all = Vec::with_capacity(queries.len());
    let t = Instant::now();
    for q in queries {
        let hits = vecs
            .search_exact_in_view(&snap, &space, q, k, Some(&candidate))
            .unwrap();
        all.push(hits.into_iter().map(|h| h.source.owner_ord).collect());
    }
    (all, t.elapsed().as_secs_f64() / queries.len() as f64)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "benchmark latency is reported per query and only needs f64 precision for printing"
)]
fn run_ann(
    vecs: &VectorEngine,
    queries: &[Vec<f32>],
    k: usize,
    ef: usize,
    allowed: &std::collections::HashSet<usize>,
) -> (Vec<Vec<u64>>, f64) {
    let snap = vecs.snapshot().unwrap();
    let space = vecs.lookup_space_in_view(&snap, "bench").unwrap().unwrap();
    let candidate = Fixture::allow_categories(allowed);
    let mut all = Vec::with_capacity(queries.len());

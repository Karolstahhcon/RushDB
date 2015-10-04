//! No-drift quality tests for the vector similarity math and its calibration.
//!
//! The engine does not embed, so a score difference between two deployments can
//! only come from the model/provider — *provided* the engine math is stable.
//! These tests pin that:
//!
//! - the scoring functions match an independently written `f64` reference across
//!   dimensions and magnitudes (the formula cannot silently change),
//! - cosine/dot/euclidean invariants and scale invariance hold,
//! - the exact-search path returns exactly the pairwise score for every hit (no
//!   hidden normalization or ingest-time transform),
//! - the two search entry points agree bit-for-bit, and
//! - scores are bit-identical across in-memory and durable backends and across a
//!   reopen (no `f32` serialization drift).

use std::collections::BTreeMap;
use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{Lmpg, RecordRef};
use rushdb_storage::Store;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{key, P_FAMILY_SIG};
use rushdb_types::occurrence::Similarity;
use rushdb_vector::{score, ScoredSource, SourceRef, VectorEngine};

const TENANT: TenantId = TenantId::from_ordinal(91);

fn metric_name(metric: Similarity) -> &'static str {
    match metric {
        Similarity::Cosine => "cosine",
        Similarity::Dot => "dot",
        Similarity::Euclidean => "euclidean",
    }
}

/// xorshift64* mapped to `[-1, 1)`, times `scale`.
#[expect(
    clippy::cast_precision_loss,
    reason = "fixture PRNG: the low 24 bits are used as a fraction, which fits f32's significand exactly"
)]
fn next_unit(state: &mut u64) -> f32 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    let unit = ((*state >> 40) as f32) / ((1u64 << 24) as f32);
    unit * 2.0 - 1.0
}

fn random_vector(state: &mut u64, dims: usize, scale: f32) -> Vec<f32> {
    (0..dims).map(|_| next_unit(state) * scale).collect()
}

/// Independent reference implementation of the three metrics, in `f64`.
fn reference_score(query: &[f32], vec: &[f32], metric: Similarity) -> f64 {
    match metric {
        Similarity::Cosine => {
            let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
            for (a, b) in query.iter().zip(vec) {
                dot += f64::from(*a) * f64::from(*b);
                na += f64::from(*a).powi(2);
                nb += f64::from(*b).powi(2);
            }
            if na == 0.0 || nb == 0.0 {
                0.0
            } else {
                dot / (na.sqrt() * nb.sqrt())
            }
        }
        Similarity::Dot => query
            .iter()
            .zip(vec)
            .map(|(a, b)| f64::from(*a) * f64::from(*b))
            .sum(),
        Similarity::Euclidean => {
            let mut dist = 0.0f64;
            for (a, b) in query.iter().zip(vec) {
                let d = f64::from(*a) - f64::from(*b);
                dist += d * d;
            }
            1.0 / (1.0 + dist.sqrt())
        }
    }
}

fn assert_close(got: f32, want: f64, ctx: &str) {
    let got = f64::from(got);
    let tol = 1e-5 * want.abs().max(1.0);
    assert!(
        (got - want).abs() <= tol,
        "{ctx}: engine {got} vs reference {want} (tol {tol})"
    );
}

fn bind_fixture(
    store: Arc<Store>,
    alias: &str,
    dims: u32,
    metric: Similarity,
    vectors: &[Vec<f32>],
) -> (VectorEngine, BTreeMap<u64, Vec<f32>>) {
    let lmpg = Lmpg::new(store.clone(), TENANT);
    let vecs = VectorEngine::new(Arc::new(Lmpg::new(store, TENANT)));
    vecs.ensure_space_similarity(alias, dims, metric).unwrap();
    let pref = key(
        TENANT,
        &[
            P_FAMILY_SIG,
            b"default",
            &[OwnerKind::Record.as_byte()],
            b"v",
        ],
    );
    let mut by_ord = BTreeMap::new();
    for v in vectors {
        let r: RecordRef = lmpg
            .create_record(
                "Doc",
                None,
                vec![("v".into(), FieldChange::Set(serde_json::json!("v")))],
            )
            .unwrap();
        let raw = lmpg.store.read_view().unwrap().get(&pref).unwrap().unwrap();
        let pid = u64::from_be_bytes(raw.as_slice().try_into().unwrap());
        let src = SourceRef {
            kind: OwnerKind::Record,
            owner_ord: r.ordinal,
            pid,
            elem: 0,
        };
        vecs.bind(alias, src, v).unwrap();
        by_ord.insert(r.ordinal, v.clone());
    }
    (vecs, by_ord)
}

fn signature(hits: &[ScoredSource]) -> Vec<(u64, u32)> {
    let mut v: Vec<(u64, u32)> = hits
        .iter()
        .map(|h| (h.source.owner_ord, h.score.to_bits()))
        .collect();
    v.sort_unstable();
    v
}

#[test]
fn score_matches_independent_reference_across_dims_and_magnitudes() {
    let mut state = 0x1234_5678_9abc_def0u64;
    for dims in [1usize, 2, 3, 8, 64, 1536] {
        for scale in [1.0f32, 1e-3, 1e3] {
            let q = random_vector(&mut state, dims, scale);
            let v = random_vector(&mut state, dims, scale);
            for metric in [Similarity::Cosine, Similarity::Dot, Similarity::Euclidean] {
                assert_close(
                    score(&q, &v, metric),
                    reference_score(&q, &v, metric),
                    &format!("dims={dims} scale={scale} {}", metric_name(metric)),
                );
            }
        }
    }
}

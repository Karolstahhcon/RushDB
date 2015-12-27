//! How a hybrid search combines its legs' rankings into one.
//!
//! - **RRF** (reciprocal rank fusion): `Σ weight / (rrfK + rank + 1)`, ranks
//!   from 0. Only ranks cross legs, so their scales never matter.
//! - **DBSF** (distribution-based score fusion, as Qdrant defines it): each
//!   leg's scores are normalized by that leg's own distribution,
//!   `(s − (μ − 3σ)) / 6σ` clamped to `[0, 1]`, then `Σ weight × normalized`.
//!   A leg whose scores do not spread (one hit, or all equal) normalizes them
//!   to 0.5.
//!
//! Equal fused scores are ordered by record, so pages are stable.

use rushdb_protocol::Fusion;

/// One leg's ranking, best first: `(record ordinal, raw score)`.
pub(super) struct Leg<'a> {
    /// `vector` or `lexical`, reported in provenance.
    pub name: &'static str,
    pub weight: f64,
    pub hits: &'a [(u64, f64)],
}

/// One fused record.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Fused {
    pub owner_ord: u64,
    pub score: f64,
    /// `(leg, rank)` for every leg that returned the record.
    pub provenance: Vec<(&'static str, usize)>,
}

/// The best `top_n` records of `legs` combined by `method`.
pub(super) fn fuse(legs: &[Leg<'_>], method: Fusion, rrf_k: f64, top_n: usize) -> Vec<Fused> {
    let mut fused: std::collections::BTreeMap<u64, Fused> = std::collections::BTreeMap::new();
    for leg in legs {
        let normalize = match method {
            Fusion::Rrf => None,
            Fusion::Dbsf => Some(distribution(leg.hits)),
        };
        for (rank, &(owner_ord, score)) in leg.hits.iter().enumerate() {
            let contribution = match normalize {
                None => leg.weight / (rrf_k + rank_f64(rank) + 1.0),
                Some((low, spread)) => {
                    let normalized = if spread > 0.0 {
                        ((score - low) / spread).clamp(0.0, 1.0)
                    } else {
                        0.5
                    };
                    leg.weight * normalized
                }
            };

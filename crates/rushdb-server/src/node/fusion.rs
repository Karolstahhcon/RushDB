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
            let entry = fused.entry(owner_ord).or_insert(Fused {
                owner_ord,
                score: 0.0,
                provenance: Vec::new(),
            });
            entry.score += contribution;
            entry.provenance.push((leg.name, rank));
        }
    }
    let mut out: Vec<Fused> = fused.into_values().collect();
    out.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then(a.owner_ord.cmp(&b.owner_ord))
    });
    out.truncate(top_n);
    out
}

/// `(μ − 3σ, 6σ)` of a leg's scores: the low end and width DBSF maps to
/// `[0, 1]`.
fn distribution(hits: &[(u64, f64)]) -> (f64, f64) {
    if hits.is_empty() {
        return (0.0, 0.0);
    }
    let count = rank_f64(hits.len());
    let mean = hits.iter().map(|(_, score)| score).sum::<f64>() / count;
    let variance = hits
        .iter()
        .map(|(_, score)| (score - mean).powi(2))
        .sum::<f64>()
        / count;
    let deviation = variance.sqrt();
    (mean - 3.0 * deviation, 6.0 * deviation)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "ranks and hit counts are bounded by the prefetch limit, far below f64's exact-integer range"
)]
fn rank_f64(rank: usize) -> f64 {
    rank as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leg<'a>(name: &'static str, weight: f64, hits: &'a [(u64, f64)]) -> Leg<'a> {
        Leg { name, weight, hits }
    }

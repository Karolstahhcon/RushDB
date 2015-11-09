//! Structural embeddings: one vector per record of a label, derived from the
//! graph around it, so records in similar positions (same community, same
//! neighbourhood shape) are near each other in a vector search.
//!
//! The model is `FastRP` (Chen et al., "Fast and Accurate Network Embeddings
//! via Very Sparse Random Projection", 2019), the algorithm Neo4j GDS ships:
//! every record gets a very sparse random vector `R`; one hop averages the
//! neighbours' `R` (`X1`), two hops average the neighbours' normalized `X1`
//! (`X2`); the embedding is `w0·R + X1 + X2`, normalized. Relationships of
//! every type count, in both directions. It is a derived, rebuildable
//! computation over the canonical graph, labeled by [`STRUCTURAL_MODEL`]:
//! the vectors are model output, not facts.
//!
//! Deterministic: `R` is a hash of the record ordinal and a fixed seed, and
//! every sum runs in ascending ordinal order, so the same graph at the same
//! commit yields bit-identical vectors.

use std::collections::{BTreeMap, BTreeSet};

use rushdb_lmpg::reader::try_access as access;
use rushdb_lmpg::reader::TryRead;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::index::{IndexDef, IndexKind};

use crate::{SourceRef, VectorEngine, VectorError};

/// Name and version of the derivation, recorded with every structural index.
pub const STRUCTURAL_MODEL: &str = "fastrp-v1";

/// Fixed seed of the random projection.
const SEED: u64 = 0x5eed_fa57_0000_0001;
/// Weight of a record's own random vector (`FastRP`'s node self-influence),
/// so a record without relationships still has a direction of its own.
const SELF_WEIGHT: f32 = 0.1;
/// Records one applier step embeds.
const APPLY_BATCH: usize = 1_024;

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// The very sparse random vector of record `ord` (sparsity 3: each component
/// is `+√3` or `-√3` with probability 1/6 each, else 0), normalized. Never
/// zero: an all-zero draw sets one component.
fn initial(ord: u64, dims: usize) -> Vec<f32> {
    let scale = 3f32.sqrt();
    let mut out: Vec<f32> = (0..dims as u64)
        .map(|component| {
            match splitmix(SEED ^ ord.wrapping_mul(0x2545_f491_4f6c_dd1d) ^ component) % 6 {
                0 => scale,
                1 => -scale,
                _ => 0.0,
            }
        })
        .collect();
    if out.iter().all(|value| *value == 0.0) {
        let pick = usize::try_from(splitmix(SEED ^ ord) % dims as u64).unwrap_or(0);
        out[pick] = scale;
    }
    normalize(&mut out);
    out
}

fn normalize(vector: &mut [f32]) {
    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in vector.iter_mut() {
            *value /= norm;
        }
    }
}

/// Records adjacent to `ord` in either direction, ascending, each once.
fn neighbours<R: TryRead + ?Sized>(r: &R, t: TenantId, ord: u64) -> Result<Vec<u64>, VectorError> {
    let mut out: Vec<u64> = access::adj_out(r, t, ord, None)?
        .into_iter()
        .chain(access::adj_in(r, t, ord, None)?)
        .map(|(_, other, _)| other)
        .collect();
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// Mean of `rows`, normalized; zero when there are none.
fn mean_normalized<'a>(dims: usize, rows: impl Iterator<Item = &'a Vec<f32>>) -> Vec<f32> {
    let mut sum = vec![0.0f32; dims];
    let mut count = 0usize;
    for row in rows {
        for (acc, value) in sum.iter_mut().zip(row) {
            *acc += value;
        }
        count += 1;
    }
    if count > 0 {
        normalize(&mut sum);
    }
    sum
}

/// Bytes [`embed`] holds per record it keeps a vector for.
fn bytes_per_vector(dims: usize) -> usize {
    dims.saturating_mul(std::mem::size_of::<f32>())
        .saturating_add(64)
}

/// The structural embedding of each record in `targets` (ascending
/// ordinals), at `view`. The working set (one-hop vectors of every record
/// within reach) is reserved from `budget` before it is computed: over
/// budget is an [`VectorError::AnnBudget`] refusal.
///
/// # Errors
/// Returns [`VectorError::Storage`] when a read fails and
/// [`VectorError::AnnBudget`] when the working set does not fit.
pub fn embed<R: TryRead + ?Sized>(
    view: &R,
    tenant: TenantId,
    targets: &[u64],
    dims: usize,
    budget: &rushdb_storage::resources::ByteBudget,
) -> Result<Vec<(u64, Vec<f32>)>, VectorError> {
    let mut adjacency: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

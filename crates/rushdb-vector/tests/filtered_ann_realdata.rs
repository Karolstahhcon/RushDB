//! Real-embedding calibration benchmark for filtered ANN. Runs the filtered-ANN path against the frozen
//! representative fixtures produced by `scripts/vector-fixtures/fetch_fixtures.sh`:
//!
//! - **DBPedia-100K** (`OpenAI` text-embedding-3-large, 1536-dim) + payload CSV:
//!   measures filtered recall/latency using a real metadata predicate (title
//!   contains a keyword -> correlated filter) and an uncorrelated synthetic
//!   category, across selectivity.
//! - **SIFT1M** (128-dim) + ground-truth `neighbors.ivecs`: measures
//!   unfiltered recall@10/@100 against the dataset's exact ground truth
//!   (the classic ANN gate), plus filtered recall with synthetic categories.
//!
//! Opt-in, like the synthetic benchmark: `RUSHDB_FILTERED_ANN_BENCH=1 cargo test
//! -p rushdb-vector --test filtered_ann_realdata --release -- --ignored`.
//! Expect several minutes and ~1 GB resident for the `DBPedia` case.

use std::sync::Arc;
use std::time::Instant;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{reader::TryRead, Lmpg, RecordRef};
use rushdb_storage::Store;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{key, P_FAMILY_SIG};
use rushdb_vector::{SourceRef, VectorEngine};

/// Where `fetch_fixtures.sh` writes the converted fixtures.
const FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../data/vector-fixtures");

/// Read an `.fvecs` file: uint32 dim then dim float32s, repeated. Returns
/// (dims, rows) with rows in f32.
fn read_fvecs(path: &str, max_rows: usize) -> (usize, Vec<Vec<f32>>) {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut pos = 0usize;
    let mut dims = 0usize;
    let mut rows = Vec::new();
    while pos + 4 <= bytes.len() && rows.len() < max_rows {
        let d = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        if dims == 0 {
            dims = d;
        }
        assert_eq!(d, dims, "inconsistent dims in {path}");
        let need = d * 4;
        let slice = &bytes[pos + 4..pos + 4 + need];
        let mut v = Vec::with_capacity(d);
        for c in slice.as_chunks::<4>().0 {
            v.push(f32::from_le_bytes(*c));
        }
        rows.push(v);
        pos += 4 + need;
    }
    (dims, rows)
}

/// Read an `.ivecs` ground-truth file: uint32 k then k int32 ids.
fn read_ivecs(path: &str) -> Vec<Vec<u32>> {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut pos = 0usize;
    let mut out = Vec::new();
    while pos + 4 <= bytes.len() {
        let k = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let mut row = Vec::with_capacity(k);
        for _ in 0..k {
            row.push(u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()));
            pos += 4;
        }
        out.push(row);
    }
    out
}

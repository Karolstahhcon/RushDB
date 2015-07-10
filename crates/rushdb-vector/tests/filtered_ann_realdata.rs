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

/// Bind real vectors into an in-memory engine, one record per vector.
fn bind_real(vectors: &[Vec<f32>], dims: usize) -> (VectorEngine, Vec<u64>) {
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store.clone(), TenantId::from_ordinal(72));
    let vecs = VectorEngine::new(Arc::new(Lmpg::new(store, TenantId::from_ordinal(72))));
    vecs.ensure_space("real", u32::try_from(dims).unwrap())
        .unwrap();

    let mut ords = Vec::with_capacity(vectors.len());
    for (i, vector) in vectors.iter().enumerate() {
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
        vecs.bind("real", src, vector).unwrap();
        ords.push(r.ordinal);
    }
    (vecs, ords)
}

/// Filter predicate: allow ordinals whose category is in `allowed`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "ordinals are reduced modulo a small constant, so the result always fits a 32-bit usize"
)]
fn allow_categories(allowed: &std::collections::HashSet<usize>) -> impl Fn(SourceRef) -> bool + '_ {
    move |src| allowed.contains(&((src.owner_ord as usize) % 20))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "recall and per-query latency are reported metrics that only need f64 precision"
)]
fn run_filtered_experiment(
    vecs: &VectorEngine,
    queries: &[Vec<f32>],
    k: usize,
    allowed: &std::collections::HashSet<usize>,
) -> (f64, f64, f64, f64) {
    let snap = vecs.snapshot().unwrap();
    let space = vecs.lookup_space_in_view(&snap, "real").unwrap().unwrap();
    let candidate = allow_categories(allowed);

    let t_exact = Instant::now();
    let mut exact: Vec<Vec<u64>> = Vec::new();
    for q in queries {
        let hits = vecs
            .search_exact_in_view(&snap, &space, q, k, Some(&candidate))
            .unwrap();

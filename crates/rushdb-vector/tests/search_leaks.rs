//! Searching holds nothing: thousands of exact, ANN and thresholded searches
//! over an on-disk store leave the shared cache budget, the resident ANN
//! bytes and the store's files exactly as the first warm search left them.
//! (The `family_soak` bench measures the same over time, as an RSS slope.)

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::resources::StorageResources;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_vector::search::{SearchMode, SearchPath};
use rushdb_vector::{SourceRef, VectorEngine};
use serde_json::json;

const SEARCHES: usize = 2_000;

/// Every file under `dir` except the store's own database and log, which
/// legitimately change as the store runs.
fn files(dir: &Path) -> BTreeSet<PathBuf> {
    let mut found = BTreeSet::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.insert(path);
            }
        }
    }
    found
}

#[test]
fn repeated_searches_hold_no_memory_and_leave_no_files() {
    let dir = rushdb_storage::TempPath::dir("vector-search-leaks");
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(

//! 3A — measured physical compaction. Many hot-key updates grow retained MVCC
//! history; `Store::compact` must reclaim physical disk space measurably while
//! preserving current + post-compaction reads.

use std::path::PathBuf;
use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("rushdb-{name}-{}", uuid::Uuid::now_v7()))
}

#[test]
fn compaction_measurably_reclaims_disk_after_hot_key_updates() {

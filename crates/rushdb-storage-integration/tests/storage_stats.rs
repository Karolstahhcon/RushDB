//! Growth accounting: reconcile per-family durable bytes with the file and
//! the commit count, so operators can see where growth comes from.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::{CompactOutcome, Store};
use rushdb_types::ids::TenantId;
use serde_json::json;

const TENANT: TenantId = TenantId::from_ordinal(77);
const COMMITS: u64 = 2_000;

fn path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rushdb-stats-{name}-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7()
    ))
}

#[test]
fn storage_stats_account_for_every_stored_byte() {
    let dir = path("account");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("rushdb.redb");
    let store = Arc::new(Store::open(&db).unwrap());
    let lmpg = Lmpg::new(store.clone(), TENANT);
    for i in 0..COMMITS {
        lmpg.create_record("S", None, vec![("u".into(), FieldChange::Set(json!(i)))])
            .unwrap();
    }

    let stats = store.storage_stats().unwrap();
    let file = store.storage_file_bytes().unwrap();
    eprintln!("stats = {stats:?}");
    eprintln!("file  = {file} bytes");

    // One retry receipt plus its expiry row per commit. The command
    // journal itself is not a store row (it is derived from the
    // WAL), so `stats.log_keys`/`log_bytes` are always 0 for a durable store.
    assert_eq!(stats.receipt_keys, 2 * COMMITS, "one receipt per commit");
    assert_eq!(stats.log_keys, 0, "no in-redb command journal rows remain");
    // Canonical data dominates key count (record header, postings, …).
    assert!(stats.data_keys > stats.receipt_keys);
    assert!(stats.data_bytes > 0);

    let live = stats.data_bytes + stats.history_bytes + stats.log_bytes + stats.receipt_bytes;
    assert!(
        file < live * 8,
        "file {file} grew more than 8x live data {live}"
    );
    store.compact_if_idle().unwrap();
    let compacted = store.storage_file_bytes().unwrap();
    eprintln!("compaction: {file} -> {compacted} bytes (live {live})");
    assert!(
        compacted < live * 3,
        "compaction left excessive slack: {compacted} vs live {live}"
    );
    {
        // The file is an uncompressed copy-on-write high-water mark, at
        // least the live rows, and idle compaction only shrinks it.
        assert!(file >= live, "file {file} < live {live}");
        assert!(
            compacted >= live,
            "compaction dropped below live bytes: {compacted} < {live}"
        );
        assert!(
            compacted <= file,
            "compaction grew the file: {file} -> {compacted}"
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compaction_is_gated_by_quiescence_and_growth() {
    use std::time::Duration;

    let dir = path("gate");
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(&dir.join("rushdb.redb")).unwrap());
    let lmpg = Lmpg::new(store.clone(), TENANT);
    for i in 0..500u64 {
        lmpg.create_record("S", None, vec![("u".into(), FieldChange::Set(json!(i)))])
            .unwrap();
    }

    // Not quiescent: a commit just happened, so compaction defers.
    assert_eq!(
        store

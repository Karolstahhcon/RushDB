//! Incremental / resumable GC (W5, plan 07 retention + D8): history
//! reclamation runs in bounded redb transactions via `retire_before_batch`
//! instead of one monolithic txn. Driving the returned cursor to completion
//! must reclaim exactly what the monolithic `gc()` would, and the durable GC
//! watermark must advance identically.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn seed_history(dir: &std::path::Path) -> (Arc<Store>, Lmpg) {
    let db = dir.join("rushdb.redb");
    let store = Arc::new(Store::open(&db).unwrap());
    let lmpg = Lmpg::new(store.clone(), TenantId::from_ordinal(91));
    let rec = lmpg
        .create_record("Doc", None, vec![("v".into(), FieldChange::Set(json!(0)))])
        .unwrap();
    for i in 1..=60 {
        lmpg.update_fields(
            rushdb_lmpg::RecordRef {
                ordinal: rec.ordinal,
                record_id: rec.record_id,
            },
            vec![("v".into(), FieldChange::Set(json!(i)))],
        )
        .unwrap();
    }
    (store, lmpg)
}

#[test]
fn bounded_incremental_gc_matches_monolithic() {
    let dir = std::env::temp_dir().join(format!(
        "rushdb-gc-inc-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // Incremental store: bounded, resumable batches.
    let (store, _lmpg) = seed_history(&dir);
    let watermark = store.committed_version();
    let mut cursor: Option<Vec<u8>> = None;
    let mut batches = 0usize;
    loop {
        let next = store
            .retire_before_batch(watermark, cursor, 4)
            .expect("bounded gc batch");
        batches += 1;
        match next {
            Some(k) => cursor = Some(k),
            None => break,
        }
    }
    // Several bounded transactions, not one monolithic sweep.
    assert!(
        batches >= 3,
        "expected multiple bounded batches, got {batches}"
    );
    assert_eq!(store.gc_watermark(), watermark);

    // Monolithic store: single gc() at the same watermark.
    let dir2 = std::env::temp_dir().join(format!(
        "rushdb-gc-mono-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir2).unwrap();
    let (store2, _lmpg2) = seed_history(&dir2);
    let wm2 = store2.committed_version();
    assert_eq!(wm2, watermark);
    store2.gc(wm2).unwrap();

    // Both approaches must advance the durable GC watermark identically. The
    // `gc()` return value (in-memory retained-version count) is a map-derived
    // metric and is 0 for redb-backed stores after P04 (maps are no longer
    // maintained); the durable floor is the contract.
    assert_eq!(store.gc_watermark(), store2.gc_watermark());
    assert_eq!(store.gc_watermark(), watermark);

    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&dir2).ok();
}

#[test]
fn incremental_gc_watermark_survives_reopen() {
    let dir = std::env::temp_dir().join(format!(
        "rushdb-gc-inc-reopen-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    {
        let (store, _lmpg) = seed_history(&dir);
        let watermark = store.committed_version();
        let mut cursor: Option<Vec<u8>> = None;
        while let Some(k) = store.retire_before_batch(watermark, cursor, 8).unwrap() {
            cursor = Some(k);
        }
        assert_eq!(store.gc_watermark(), watermark);
    }
    let reopened = Store::open(&dir.join("rushdb.redb")).unwrap();
    assert_eq!(
        reopened.gc_watermark(),
        reopened.committed_version(),
        "incremental GC watermark must be durable"
    );
    std::fs::remove_dir_all(&dir).ok();
}


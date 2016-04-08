//! Durability A: a physical base backup plus continuously archived WAL
//! restores any point in the retained window (base + PITR), so the store is no
//! longer "one file or nothing".

use rushdb_server::archive;
use rushdb_server::{Node, ServerConfig};
use serde_json::json;

fn config(dir: &std::path::Path) -> ServerConfig {
    ServerConfig {
        data_dir: Some(dir.to_path_buf()),
        ..Default::default()
    }
}

fn snapshot_bytes(node: &Node) -> Vec<u8> {
    let mut out = Vec::new();
    node.export_snapshot(&mut out, &[]).unwrap();
    out
}

fn write_range(node: &Node, from: u64, to: u64) {
    for i in from..to {
        node.scope()
            .create_record("S", &json!({ "u": i }), &[], &json!({}))
            .unwrap();
    }
}

#[test]
fn base_backup_plus_archived_wal_restores_to_a_point_in_time() {
    let root = std::env::temp_dir().join(format!(
        "rushdb-pitr-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let src_dir = root.join("src");
    let archive_dir = root.join("archive");
    let base_dir = root.join("base");
    let restored60 = root.join("restored60");
    let restored45 = root.join("restored45");
    std::fs::create_dir_all(&src_dir).unwrap();

    let (snap30, snap45, snap60);
    {
        let node = Node::bootstrap(&mut config(&src_dir)).unwrap();
        write_range(&node, 0, 30);
        assert_eq!(node.committed_version(), 30);
        snap30 = snapshot_bytes(&node);
        // Archive the first range, then take the physical base at commit 30.
        // Log-first: checkpoint the materialization before the offline base
        // copy so the copied store file is durable at commit 30.
        node.archive_wal(&archive_dir).unwrap();
        node.checkpoint().unwrap();
        archive::backup_base(&src_dir, &base_dir, 30).unwrap();

        write_range(&node, 30, 45);
        snap45 = snapshot_bytes(&node);
        node.archive_wal(&archive_dir).unwrap();

        write_range(&node, 45, 60);
        snap60 = snapshot_bytes(&node);
        node.archive_wal(&archive_dir).unwrap();
        assert!(snap30 != snap45 && snap45 != snap60);
    }

    // Restore the base at 30 and replay the archive to the latest (60).
    archive::restore_base_into(&base_dir, &restored60).unwrap();
    assert!(archive::restore_base_into(&base_dir, &restored60).is_err());
    {
        let node = Node::bootstrap(&mut config(&restored60)).unwrap();
        assert_eq!(node.committed_version(), 30, "base pinned at 30");
        let mut stream = archive::segment_stream(&archive_dir).unwrap();
        let ci = node.restore_pitr_to(&mut stream, Some(30), None).unwrap();
        assert_eq!(ci, 60);
        // G2: the live replay derives every commit's counters, presence and
        // dictionary rows; `verify` compares them with a from-scratch
        // derivation of the restored base rows.
        assert_eq!(node.verify().unwrap(), 0, "restored graph is consistent");
        assert_eq!(snapshot_bytes(&node), snap60, "state matches source at 60");
    }

    // Point-in-time restore to 45 from the same base + archive.
    archive::restore_base_into(&base_dir, &restored45).unwrap();
    {
        let node = Node::bootstrap(&mut config(&restored45)).unwrap();
        let mut stream = archive::segment_stream(&archive_dir).unwrap();
        let ci = node
            .restore_pitr_to(&mut stream, Some(30), Some(45))
            .unwrap();
        assert_eq!(ci, 45);
        assert_eq!(node.verify().unwrap(), 0, "derived rows exact at 45");
        assert_eq!(snapshot_bytes(&node), snap45, "state matches source at 45");
    }

    // The base manifest verifies, and a corrupted base file is rejected.
    assert_eq!(archive::verify_base(&base_dir).unwrap(), 30);
    let base_file = base_dir.join("base").join("rushdb.redb");
    let mut bytes = std::fs::read(&base_file).unwrap();
    if let Some(byte) = bytes.last_mut() {
        *byte ^= 0xFF;
    }
    std::fs::write(&base_file, &bytes).unwrap();
    assert!(
        archive::verify_base(&base_dir).is_err(),
        "corruption detected"
    );

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn online_backup_under_concurrent_writes_restores_consistently() {
    // An online base backup, taken while the
    // server keeps accepting writes, must still restore to a consistent
    // state exactly at the commit index it reports — unlike
    // `--backup-base`, nothing stops the writer for this to hold.
    let root = rushdb_storage::TempPath::dir("online-backup");

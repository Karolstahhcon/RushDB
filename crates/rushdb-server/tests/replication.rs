//! Streaming replication. A follower polls a leader's
//! internal WAL-tail endpoint and replays each commit batch locally; this
//! proves it converges on pre-existing data and stays converged under
//! concurrent writes on the leader.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rushdb_server::{http, Node, ServerConfig};
use serde_json::json;

fn config(dir: &std::path::Path, token: &str) -> ServerConfig {
    let mut cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: token.to_string(),
        data_dir: Some(dir.to_path_buf()),
        ..Default::default()
    };
    // These followers start empty and tail the leader from LSN 1. With the
    // default retention (keep only what a registered consumer needs) the
    // leader may prune that history before a follower's first poll, so the
    // leader keeps a window, as an operator would for bootstrap-from-empty.
    // Attaching to a leader that has pruned is the base-backup path
    // (`a_restored_base_backup_lets_a_follower_attach_mid_stream`).
    cfg.resources.tx_log_keep_commits = 100_000;
    cfg
}

fn write_range(node: &Node, from: u64, to: u64) {
    for i in from..to {
        node.scope()
            .create_record("S", &json!({ "u": i }), &[], &json!({}))
            .unwrap();
    }
}

/// Decode an `export_snapshot` stream into its tenant JSONL lines, sorted —
/// matching this project's own memory-vs-disk differential comparison
/// (`memory_disk_differential.rs::normalize`), since row export order is a
/// scan artifact, not a content guarantee.
fn normalized_lines(snapshot: &[u8]) -> Vec<String> {
    use rushdb_storage::wal::{decode_tenant_chunk, read_framed_chunk, read_snapshot_manifest};
    let mut cursor = std::io::Cursor::new(snapshot);
    let manifest = read_snapshot_manifest(&mut cursor).unwrap();
    let mut lines = Vec::new();
    for _ in 0..manifest.tenant_count {
        let raw = read_framed_chunk(&mut cursor).unwrap().unwrap();
        let chunk = decode_tenant_chunk(&raw).unwrap();
        let jsonl = String::from_utf8(chunk.jsonl).unwrap();
        lines.extend(jsonl.lines().map(str::to_string));
    }
    lines.sort();
    lines
}

fn wait_for_commit(node: &Node, target: u64, deadline: std::time::Instant) {
    while node.committed_version() < target {
        assert!(
            std::time::Instant::now() <= deadline,
            "follower did not catch up: follower={} leader={}",
            node.committed_version(),
            target
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn streaming_replication_follower_converges_under_concurrent_writes() {
    const TOKEN: &str = "shared-replication-test-token";
    let root = rushdb_storage::TempPath::dir("replication");
    let leader_dir = root.join("leader");
    let follower_dir = root.join("follower");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();

    // Pre-existing data: the follower must bootstrap-replicate this before
    // it ever sees a "live" write.
    let leader_node = Arc::new(Node::bootstrap(&mut config(&leader_dir, TOKEN)).unwrap());
    write_range(&leader_node, 0, 50);

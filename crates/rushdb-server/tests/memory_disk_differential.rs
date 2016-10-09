//! 3A — memory-vs-disk differential suite. The same operation script must
//! produce the identical committed state on a memory node and a disk-backed
//! node, survive reopen on disk, and replay historical states identically.
//! This is the direct proof that redb mode shares memory-mode read semantics.

use rushdb_protocol::{Mutation, Operation, RequestContext, RequestEnvelope, Response};
use rushdb_server::{Node, ServerConfig};

fn disk_node(dir: &std::path::Path) -> Node {
    let mut cfg = ServerConfig {
        data_dir: Some(dir.to_path_buf()),
        ..Default::default()
    };
    Node::bootstrap(&mut cfg).unwrap()
}

fn memory_node() -> Node {
    let mut cfg = ServerConfig::default();
    Node::bootstrap(&mut cfg).unwrap()
}

fn put(node: &Node, _id: &str, label: &str, text: &str) {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("diff-put".into()),
        context: RequestContext::default(),
        operation: Operation::Mutate {
            mutation: Mutation::PutRecord {
                mode: rushdb_protocol::PutMode::Create,
                id: None,
                label: Some(label.to_string()),
                properties: serde_json::json!({ "text": text }),
                vectors: vec![],
                options: serde_json::json!({}),
            },
        },
    };
    rushdb_server::protocol::ProtocolExecutor::new(node, None)
        .execute(envelope)
        .unwrap();
}

fn delete(node: &Node, id: &str) {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("diff-del".into()),
        context: RequestContext::default(),
        operation: Operation::Mutate {
            mutation: Mutation::DeleteRecord { id: id.to_string() },
        },
    };
    rushdb_server::protocol::ProtocolExecutor::new(node, None)
        .execute(envelope)
        .unwrap();
}

/// Normalize exported JSONL for comparison: drop the auto-generated record id
/// so memory and disk modes compare on logical content.
fn normalize(jsonl: Vec<u8>) -> Vec<String> {
    let mut out: Vec<String> = String::from_utf8(jsonl)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let mut v: serde_json::Value = serde_json::from_str(l).unwrap();
            if let Some(o) = v.as_object_mut() {
                o.remove("id");
            }
            serde_json::to_string(&v).unwrap()
        })
        .collect();
    out.sort();
    out
}

/// Normalize all tenants' JSONL into one comparable set.
fn normalize_all(jsonl: Vec<Vec<u8>>) -> Vec<String> {
    let mut out = Vec::new();
    for tenant in jsonl {
        out.extend(normalize(tenant));

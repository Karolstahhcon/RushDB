//! Vector search mode: `auto` answers exactly up to
//! `resources.vector-exact-max-pool` vectors and on the ANN graph above it,
//! never waiting for a build; `exact`/`ann` force a path; `scoreThreshold`
//! drops weak hits; the response reports the path and the index's pool.

use rushdb_protocol::{
    ErrorCode, Operation, Query, RequestContext, RequestEnvelope, Response, VectorIndexDefinition,
    VectorItem, VectorQuery, VectorSearchMode, VectorSearchReport,
};
use rushdb_server::{Node, ServerConfig};

const EXACT_MAX_POOL: u64 = 4;

fn node() -> Node {
    let mut cfg = ServerConfig::default();
    cfg.resources.vector_exact_max_pool = EXACT_MAX_POOL;
    Node::bootstrap(&mut cfg).unwrap()
}

fn index(n: &Node, name: &str, label: &str, similarity: &str) {
    n.scope()
        .create_vector_index(&VectorIndexDefinition {
            name: Some(name.into()),
            label: label.into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: similarity.into(),
            model: None,
            owner: None,
        })
        .unwrap();
}

/// Bind one vector per angle (radians from `[1, 0]`) to a new record.
fn bind(n: &Node, name: &str, label: &str, angles: &[f32]) -> Vec<String> {
    let mut ids = Vec::new();
    let items: Vec<VectorItem> = angles
        .iter()
        .map(|angle| {
            let vector = vec![angle.cos(), angle.sin()];
            let id = n
                .scope()
                .create_record(
                    label,
                    &serde_json::json!({ "embedding": vector }),
                    &[],
                    &serde_json::json!({}),
                )
                .unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string();
            ids.push(id.clone());
            VectorItem {
                record_id: id,
                vector,
                source_hash: None,
                index_generation: None,
            }
        })
        .collect();
    n.scope().upsert_vector_index(name, &items).unwrap();
    ids
}

fn query(name: &str, label: &str) -> VectorQuery {
    VectorQuery {
        labels: vec![label.into()],
        property: "embedding".into(),
        vector: vec![1.0, 0.0],
        similarity: "cosine".into(),
        index: Some(name.into()),
        predicate: serde_json::json!({}),
        offset: 0,
        // Below the index size: an ANN gather shorter than `min(limit, ef)`
        // is answered exactly (`lowConfidence`), which a tiny index always is.
        limit: 2,
        mode: None,
        ef: None,
        score_threshold: None,
    }
}

fn search(n: &Node, query: &VectorQuery) -> Result<(usize, VectorSearchReport), ErrorCode> {
    let response = rushdb_server::protocol::ProtocolExecutor::new(n, None)
        .execute(RequestEnvelope {
            protocol_version: 1,
            request_id: None,
            context: RequestContext::default(),

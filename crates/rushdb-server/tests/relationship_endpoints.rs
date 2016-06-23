//! `query.relationships` with endpoint predicates: one record's relationships
//! come from its own links, outgoing for a source filter and incoming for a
//! target filter, without listing the store's other relationships.

use rushdb_protocol::{RelationshipIdsMutation, RelationshipQuery};
use rushdb_server::{Node, ServerConfig};
use serde_json::{json, Value};

fn node() -> Node {
    let mut cfg = ServerConfig::default();
    Node::bootstrap(&mut cfg).unwrap()
}

fn record(n: &Node, name: &str) -> String {
    n.scope()
        .create_record("N", &json!({ "name": name }), &[], &json!({}))
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn link(n: &Node, source: &str, target: &str) {
    let command: RelationshipIdsMutation = serde_json::from_value(json!({

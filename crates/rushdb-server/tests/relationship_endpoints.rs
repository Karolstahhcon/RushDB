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
        "relationType": "FLOW", "sourceIds": [source], "targetIds": [target]
    }))
    .unwrap();
    n.scope().link_records_by_id(&command).unwrap();
}

/// `(source, target)` id pairs of the matching relationships, and the total.
fn relationships(n: &Node, query: Value) -> (Vec<(String, String)>, u64) {
    let query: RelationshipQuery = serde_json::from_value(query).unwrap();
    let result = n.scope().scan_relationships(&query).unwrap();
    let pairs = result["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["sourceId"].as_str().unwrap().to_string(),
                row["targetId"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    (pairs, result["total"].as_u64().unwrap())
}

#[test]

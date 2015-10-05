//! Structural indexes end to end: declared over the protocol, built and
//! applied by the node, searched with `query.vector`, kept fresh after a
//! topology change, and dropped with their vectors.

use super::*;
use rushdb_protocol::{
    Describe, IndexDefinition, IndexOperation, RequestContext, RequestEnvelope, VectorQuery,
};

fn run(node: &Node, operation: Operation) -> Response {
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation,
        })
        .unwrap()
        .response
}

fn record(node: &Node) -> String {
    node.scope()
        .create_record("Node", &serde_json::json!({}), &[], &serde_json::json!({}))
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn link(node: &Node, source: &str, target: &str) {
    node.scope()
        .link_records_by_id(&rushdb_protocol::RelationshipIdsMutation {
            relation_type: "LINK".into(),
            direction: rushdb_protocol::Direction::Out,
            source_ids: vec![source.to_owned()],
            target_ids: vec![target.to_owned()],
            properties: serde_json::json!({}),
        })
        .unwrap();
}

/// A 6-record clique.
fn clique(node: &Node) -> Vec<String> {
    let members: Vec<String> = (0..6).map(|_| record(node)).collect();
    for (i, a) in members.iter().enumerate() {
        for b in &members[i + 1..] {
            link(node, a, b);
        }
    }
    members
}

fn structural_index(node: &Node) -> Option<serde_json::Value> {
    let Response::Indexes { indexes } = run(
        node,
        Operation::Describe {
            describe: Describe::Indexes,
        },
    ) else {
        panic!("indexes response")
    };

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
    indexes.into_iter().find(|index| index["name"] == "shape")
}

/// The 5 records nearest `vector` in the `shape` structural index.
fn nearest(node: &Node, vector: Vec<f32>) -> Vec<serde_json::Value> {
    let Response::Records { records, .. } = run(
        node,
        Operation::Query {
            query: Query::Vector {
                query: VectorQuery {
                    labels: vec!["Node".into()],
                    property: "*".into(),
                    vector,
                    similarity: "cosine".into(),
                    index: Some("shape".into()),
                    predicate: serde_json::json!({}),
                    offset: 0,
                    limit: 5,
                    ef: Some(32),
                    mode: None,
                    score_threshold: None,
                },
            },
        },
    ) else {
        panic!("records response")
    };
    records
}

fn settle(node: &Node) {
    while node.step_index_builds(std::time::Duration::from_secs(5)) > 0 {}
    while node.apply_structural_indexes(std::time::Duration::from_secs(5)) > 0 {}
}

#[test]
fn structural_index_is_searched_kept_fresh_and_dropped() {
    let node = Node::bootstrap(&mut super::tests::config(false)).unwrap();
    let a = clique(&node);
    let b = clique(&node);
    link(&node, &a[0], &b[0]);
    let Response::Index { index } = run(
        &node,
        Operation::Index {
            index: IndexOperation::Create {
                definition: IndexDefinition {
                    name: "shape".into(),
                    label: "Node".into(),
                    property: "*".into(),
                    kind: "structural".into(),
                    consistency: None,
                    dimensions: Some(32),
                },
            },
        },
    ) else {
        panic!("index response")
    };
    assert_eq!(index["kind"], "structural");
    assert_eq!(index["model"], rushdb_vector::structural::STRUCTURAL_MODEL);
    settle(&node);
    let listed = structural_index(&node).unwrap();

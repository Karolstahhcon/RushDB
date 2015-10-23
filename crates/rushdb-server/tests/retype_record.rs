//! Retype-record mutation conformance over the in-process protocol executor.
use rushdb_protocol::{
    Mutation, Operation, PutMode, Query, QueryDocument, RecordReference, RequestContext,
    RequestEnvelope, Response, TransactionMutation, TransactionOperation,
};
use rushdb_server::{Node, ServerConfig};

fn node() -> Node {
    Node::bootstrap(&mut ServerConfig::default()).unwrap()
}

fn execute(node: &Node, operation: Operation) -> Response {
    rushdb_server::protocol::ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: 1,
            request_id: Some("retype-record".into()),
            context: RequestContext::default(),
            operation,
        })
        .unwrap()
        .response
}

fn create(node: &Node, label: &str, title: &str) -> serde_json::Value {
    let Response::Record { record } = execute(
        node,
        Operation::Mutate {
            mutation: Mutation::PutRecord {
                mode: PutMode::Create,
                id: None,
                label: Some(label.into()),
                properties: serde_json::json!({ "title": title }),
                vectors: vec![],
                options: serde_json::json!({}),
            },
        },
    ) else {
        panic!("expected created record")
    };
    record
}

fn by_label(node: &Node, label: &str) -> Vec<serde_json::Value> {
    let Response::Records { records, .. } = execute(
        node,
        Operation::Query {
            query: Query::Records {
                query: QueryDocument {
                    labels: vec![label.into()],
                    limit: Some(100),
                    ..Default::default()
                },
            },
        },
    ) else {
        panic!("expected records")
    };
    records
}

#[test]
fn retype_record_preserves_identity_properties_and_relationships() {
    let node = node();
    let book = create(&node, "BOOK", "Dune");
    let author = create(&node, "AUTHOR", "Frank Herbert");

//! Request contracts: the query
//! document is exact and `putRecord` honours an explicit id, with
//! merge upserting.

use super::*;
use rushdb_protocol::{PutMode, Query, RequestContext, RequestEnvelope};
use serde_json::json;

fn run(node: &Node, operation: Operation) -> Result<Response, rushdb_protocol::ProtocolError> {
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation,
        })
        .map(|executed| executed.response)
}

fn node() -> Node {
    let mut cfg = super::tests::config(false);
    Node::bootstrap(&mut cfg).unwrap()
}

fn put(mode: PutMode, id: Option<&str>, label: Option<&str>, v: i64) -> Operation {
    Operation::Mutate {
        mutation: rushdb_protocol::Mutation::PutRecord {
            mode,
            id: id.map(str::to_string),
            label: label.map(str::to_string),
            properties: json!({"v": v}),
            vectors: Vec::new(),
            options: json!({}),
        },
    }
}

fn records(node: &Node, predicate: serde_json::Value) -> Vec<serde_json::Value> {
    let Ok(Response::Records { records, .. }) = run(
        node,
        Operation::Query {
            query: Query::Records {
                query: QueryDocument {
                    predicate,
                    ..QueryDocument::default()
                },
            },
        },
    ) else {
        panic!("expected records")
    };
    records
}

#[test]
fn an_unknown_query_document_key_is_rejected_by_name() {
    let node = node();
    node.scope()
        .create_record("Doc", &json!({"v": 1}), &[], &json!({}))
        .unwrap();
    let body = json!({
        "protocolVersion": 1,
        "operation": {"kind": "query", "query": {"kind": "records",
            "query": {"labels": ["Doc"], "where": {"v": 2}}}},

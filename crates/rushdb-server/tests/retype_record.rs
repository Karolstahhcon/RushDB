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
    let book_id = book["id"].as_str().unwrap().to_string();
    let author_id = author["id"].as_str().unwrap().to_string();

    let snap = node.scope().lmpg().store.read_view().unwrap();
    let book_header = rushdb_lmpg::reader::try_access::record_by_id(
        &snap,
        node.scope().tenant(),
        &rushdb_types::ids::RecordId(crate_id(&book_id)),
    )
    .unwrap()
    .unwrap();
    let author_header = rushdb_lmpg::reader::try_access::record_by_id(
        &snap,
        node.scope().tenant(),
        &rushdb_types::ids::RecordId(crate_id(&author_id)),
    )
    .unwrap()
    .unwrap();
    let relationship = node
        .scope()
        .lmpg()
        .create_relationship(
            "WRITTEN_BY",
            rushdb_lmpg::RecordRef {
                record_id: book_header.record_id,
                ordinal: book_header.ordinal,
            },
            rushdb_lmpg::RecordRef {
                record_id: author_header.record_id,
                ordinal: author_header.ordinal,
            },
            vec![],
        )
        .unwrap();

    let Response::Record { record } = execute(
        &node,
        Operation::Mutate {
            mutation: Mutation::RetypeRecord {
                id: book_id.clone(),
                from_label: "BOOK".into(),
                to_label: "MAGAZINE".into(),
            },
        },
    ) else {
        panic!("expected retyped record")
    };
    assert_eq!(record["id"], book_id);
    assert_eq!(record["label"], "MAGAZINE");
    assert_eq!(record["properties"]["title"], "Dune");
    assert!(by_label(&node, "BOOK").is_empty());
    assert_eq!(by_label(&node, "MAGAZINE").len(), 1);

    let snap = node.scope().lmpg().store.read_view().unwrap();
    let retyped = rushdb_lmpg::reader::try_access::record_by_id(
        &snap,
        node.scope().tenant(),
        &rushdb_types::ids::RecordId(crate_id(&book_id)),
    )
    .unwrap()
    .unwrap();
    assert_eq!(retyped.ordinal, book_header.ordinal);
    assert_eq!(retyped.label, "MAGAZINE");
    let rel = rushdb_lmpg::reader::try_access::relationship_by_ord(
        &snap,

//! Multi-step writes commit once (all or nothing): `putRecord` replace,
//! query/ID link and unlink, `retypeRelations` and Mongo `*Many`. Each test
//! caps `max-expanded-commit-bytes` between one item's commit and the whole
//! operation's, so the operation is refused partway through its items and
//! must leave nothing applied.

use super::*;
use crate::resource_config::ByteSize;
use rushdb_protocol::{
    Direction, RelationshipIdsMutation, RelationshipMutation, RelationshipQuery,
    RelationshipSelector, RequestContext, RequestEnvelope, RetypeRelationsMutation,
    StatementLanguage,
};

/// Items per operation; each test's cap fits one item's commit, not all.
const ITEMS: usize = 60;
/// Fits one write of a few hundred bytes, not [`ITEMS`] of them.
const WRITE_CAP: usize = 24 * 1024;
/// Fits one item's setup commit, not [`ITEMS`] relationship deletes.
const DELETE_CAP: usize = 8 * 1024;
/// Fits one small record, not [`ITEMS`] record updates or deletes.
const RECORD_CAP: usize = 4 * 1024;

fn capped_node(cap: usize) -> Node {
    let mut cfg = super::tests::config(false);
    cfg.resources.max_expanded_commit_bytes = ByteSize(cap);
    Node::bootstrap(&mut cfg).unwrap()
}

/// Distinct per item: identical values share one dictionary entry, so they
/// would not grow the commit.
fn filler(i: usize) -> String {
    format!("{i:04}{}", "x".repeat(508))
}

fn properties_of(node: &Node, id: &str) -> serde_json::Value {
    let view = node.store().read_view().unwrap();
    let id = rushdb_types::ids::RecordId(*uuid::Uuid::parse_str(id).unwrap().as_bytes());
    let header = rushdb_lmpg::reader::try_access::record_by_id(&view, node.scope().tenant(), &id)
        .unwrap()
        .unwrap();
    rushdb_lmpg::reconstruct::reconstruct_record(&view, node.scope().tenant(), header.ordinal)
        .unwrap()
        .unwrap()
}

fn mutate(node: &Node, mutation: Mutation) -> Result<Response, ProtocolError> {
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Mutate { mutation },
        })
        .map(|envelope| envelope.response)
}

fn assert_refused_whole(result: Result<Response, ProtocolError>) {
    let error = result.expect_err("an operation over the commit cap is refused");
    assert!(
        error.message.contains("expanded mutation batch exceeds"),
        "refused for the commit size, not something else: {}",
        error.message
    );
}

fn create(node: &Node, label: &str, properties: &serde_json::Value) -> String {
    node.scope()
        .create_record(label, properties, &[], &serde_json::json!({}))
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn relationship_count(node: &Node, relation_type: &str) -> usize {
    let rows = node
        .scope()
        .scan_relationships(&RelationshipQuery {
            source_labels: Vec::new(),

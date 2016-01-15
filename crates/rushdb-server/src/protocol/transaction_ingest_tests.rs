//! An import staged into a transaction (JSON documents or CSV) creates its
//! records and relationships on commit, and not before.

use super::transaction_reference_tests::run;
use super::*;

fn begin(node: &Node) -> String {
    let begun = run(
        node,
        &serde_json::json!({"kind": "transaction", "transaction": {"kind": "begin"}}),
    )
    .unwrap();
    begun["transactionId"].as_str().unwrap().to_owned()
}

fn transaction(node: &Node, tx: &str, kind: &str, extra: &serde_json::Value) -> serde_json::Value {
    let mut transaction = serde_json::json!({"kind": kind, "transactionId": tx});
    transaction
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    run(
        node,
        &serde_json::json!({"kind": "transaction", "transaction": transaction}),
    )
    .unwrap()
}

fn stage_ingest(
    node: &Node,
    tx: &str,
    ingest: &serde_json::Value,
) -> Result<serde_json::Value, ProtocolError> {
    run(
        node,
        &serde_json::json!({"kind": "transaction", "transaction": {
            "kind": "stage", "transactionId": tx,
            "mutation": {"kind": "ingest", "ingest": ingest},
        }}),
    )
}

fn count(node: &Node, label: &str) -> usize {
    let response = run(
        node,
        &serde_json::json!({"kind": "query", "query": {"kind": "records", "query": {"labels": [label]}}}),
    )
    .unwrap();
    response["records"].as_array().unwrap().len()
}

fn links(node: &Node) -> usize {
    let response = run(
        node,
        &serde_json::json!({"kind": "query", "query": {"kind": "relationships", "query": {}}}),
    )
    .unwrap();
    response["relationships"].as_array().unwrap().len()
}

#[test]
fn a_staged_json_import_lands_on_commit_with_its_relationships() {
    let node = Node::bootstrap(&mut super::tests::config(false)).unwrap();
    let tx = begin(&node);
    let staged = stage_ingest(
        &node,
        &tx,
        &serde_json::json!({"kind": "documents", "label": "Author", "documents": [

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

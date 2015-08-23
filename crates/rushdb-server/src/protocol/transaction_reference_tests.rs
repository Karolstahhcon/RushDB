//! Transaction stages name existing records by public id alone: the engine
//! resolves each id inside the transaction, so committed records and records
//! staged earlier in the same transaction can be updated, deleted and linked.

use super::*;

pub(super) fn run(
    node: &Node,
    operation: &serde_json::Value,
) -> Result<serde_json::Value, ProtocolError> {
    let envelope: RequestEnvelope = serde_json::from_value(serde_json::json!({
        "protocolVersion": PROTOCOL_VERSION,
        "operation": operation,
    }))
    .unwrap();
    ProtocolExecutor::new(node, None)
        .execute(envelope)
        .map(|envelope| serde_json::to_value(envelope.response).unwrap())
}

fn begin(node: &Node) -> String {
    let begun = run(
        node,
        &serde_json::json!({"kind": "transaction", "transaction": {"kind": "begin"}}),
    )
    .unwrap();
    begun["transactionId"].as_str().unwrap().to_owned()
}

fn stage(
    node: &Node,
    tx: &str,
    mutation: &serde_json::Value,
) -> Result<serde_json::Value, ProtocolError> {
    run(
        node,
        &serde_json::json!({"kind": "transaction", "transaction": {
            "kind": "stage", "transactionId": tx, "mutation": mutation,
        }}),
    )
}

fn commit(node: &Node, tx: &str) -> serde_json::Value {
    run(
        node,
        &serde_json::json!({"kind": "transaction", "transaction": {
            "kind": "commit", "transactionId": tx,
        }}),
    )
    .unwrap()
}

fn record(node: &Node, id: &str) -> Option<serde_json::Value> {
    run(
        node,
        &serde_json::json!({"kind": "query", "query": {"kind": "record", "id": id}}),
    )
    .ok()
    .and_then(|response| response.get("record").cloned())

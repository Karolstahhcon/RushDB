//! MERGE-1 (b) on the wire: a reserved (`__`) property name or `mergeBy` key, and
//! an upsert on a key without an index over `unindexed-walk-max-owners`, are
//! the client's to fix: `invalid_request`, not retryable.

use super::*;
use rushdb_protocol::{RequestContext, RequestEnvelope};

fn put(node: &Node, mutation: &serde_json::Value) -> Result<Response, ProtocolError> {
    let operation: Operation =
        serde_json::from_value(serde_json::json!({ "kind": "mutate", "mutation": mutation }))
            .unwrap();
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: None,
                deadline_ms: None,
                ..Default::default()
            },
            operation,
        })
        .map(|envelope| envelope.response)
}

fn create(properties: &serde_json::Value, merge_by: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "kind": "putRecord", "mode": "create", "label": "User",
        "properties": properties, "options": { "mergeBy": merge_by }
    })
}

fn assert_invalid(error: &ProtocolError, expected: &str) {
    assert_eq!(error.code, ErrorCode::InvalidRequest, "{error:?}");
    assert!(!error.retryable, "{error:?}");
    assert!(error.message.contains(expected), "{error:?}");
}

#[test]
fn reserved_names_and_unindexed_upserts_over_the_limit_are_invalid_requests() {
    let mut config = super::tests::config(false);

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

//! QRY-3 on the wire: a filter without a usable index runs by default, like
//! a PostgreSQL sequential scan; a request may bound it with
//! `context.unindexedWalkMaxOwners` and `context.queryMemoryMaxBytes`, which
//! only tighten the server's ceilings.

use super::*;
use rushdb_protocol::{RequestContext, RequestEnvelope};

fn execute(
    node: &Node,
    context: RequestContext,
    operation: &serde_json::Value,
) -> Result<Response, ProtocolError> {
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context,
            operation: serde_json::from_value(operation.clone()).unwrap(),
        })
        .map(|envelope| envelope.response)
}

fn walk_limited(owners: u64) -> RequestContext {
    RequestContext {
        unindexed_walk_max_owners: Some(owners),
        ..Default::default()
    }
}

fn filtered_read() -> serde_json::Value {
    serde_json::json!({"kind": "query", "query": {"kind": "records", "query": {
        "labels": ["User"], "predicate": {"n": 1}}}})
}

fn sql_count() -> serde_json::Value {

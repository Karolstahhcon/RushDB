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
    serde_json::json!({"kind": "query", "query": {"kind": "statement", "language": "sql",
        "statement": "SELECT count(*) AS c FROM \"user\" WHERE n = 1"}})
}

fn node_with_users(configure: impl FnOnce(&mut crate::ServerConfig)) -> Node {
    let mut config = super::tests::config(false);
    config.languages.sql = true;
    configure(&mut config);
    let node = Node::bootstrap(&mut config).unwrap();
    for n in 0..5 {
        execute(
            &node,
            RequestContext::default(),
            &serde_json::json!({"kind": "mutate", "mutation": {"kind": "putRecord",
                "mode": "create", "label": "User", "properties": {"n": n, "email": format!("u{n}")},
                "options": {}}}),
        )
        .unwrap();
    }
    node
}

fn assert_refused_walk(result: Result<Response, ProtocolError>) {
    let error = result.unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidRequest, "{error:?}");
    assert!(
        error.message.contains("declare an ordered index"),
        "{error:?}"
    );
}

#[test]
fn an_unindexed_filter_runs_unless_the_request_bounds_it() {
    let node = node_with_users(|_| {});
    // The server sets no limit.
    assert!(execute(&node, RequestContext::default(), &filtered_read()).is_ok());

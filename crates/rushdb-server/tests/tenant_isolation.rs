//! Cross-tenant isolation (5B): fail-closed, scope-keyed multitenancy must not
//! leak data, backups, or transactions across the opaque scope boundary. These
//! tests drive the canonical protocol executor under explicit request scopes.

use rushdb_authz::{AuthorizationMode, DataAccessPolicy};
use rushdb_protocol::{
    Describe, ErrorCode, Mutation, Operation, PutMode, Query, QueryDocument, RequestContext,
    RequestEnvelope, Response, TransactionMutation, TransactionOperation, TransactionStatus,
};
use rushdb_server::{Node, RequestScope, ServerConfig};

fn put_mode(
    node: &Node,
    scope: &str,
    mode: PutMode,
    id: Option<&str>,
    label: Option<&str>,
    properties: serde_json::Value,
) -> Result<serde_json::Value, rushdb_protocol::ProtocolError> {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("isolation-put-mode".into()),
        context: RequestContext {
            scope: Some(scope.to_string()),
            deadline_ms: None,
            ..Default::default()
        },
        operation: Operation::Mutate {
            mutation: Mutation::PutRecord {
                mode,
                id: id.map(str::to_string),
                label: label.map(str::to_string),
                properties,
                vectors: vec![],
                options: serde_json::json!({}),
            },
        },
    };
    let response = rushdb_server::protocol::ProtocolExecutor::new(node, Some(scope.to_string()))
        .execute(envelope)?;
    let Response::Record { record } = response.response else {
        panic!("expected record response, got {:?}", response.response);
    };
    Ok(record)
}

fn multitenant() -> Node {
    let mut cfg = ServerConfig {
        multitenant: true,
        ..Default::default()
    };
    Node::bootstrap(&mut cfg).unwrap()
}

fn put(node: &Node, scope: &str, label: &str, text: &str) {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("isolation-put".into()),
        context: RequestContext {
            scope: Some(scope.to_string()),
            deadline_ms: None,
            ..Default::default()
        },
        operation: Operation::Mutate {
            mutation: Mutation::PutRecord {
                mode: rushdb_protocol::PutMode::Create,
                id: None,
                label: Some(label.to_string()),
                properties: serde_json::json!({ "text": text }),
                vectors: vec![],
                options: serde_json::json!({}),
            },
        },
    };
    rushdb_server::protocol::ProtocolExecutor::new(node, Some(scope.to_string()))
        .execute(envelope)
        .unwrap();
}

fn find_text(node: &Node, scope: &str) -> Vec<serde_json::Value> {
    let doc = QueryDocument {
        labels: vec!["NOTE".to_string()],
        limit: Some(100),
        ..Default::default()
    };
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("isolation-find".into()),
        context: RequestContext {
            scope: Some(scope.to_string()),
            deadline_ms: None,
            ..Default::default()
        },
        operation: Operation::Query {
            query: Query::Records { query: doc },
        },
    };
    let res = rushdb_server::protocol::ProtocolExecutor::new(node, Some(scope.to_string()))
        .execute(envelope)
        .unwrap();
    let Response::Records { records, .. } = res.response else {
        panic!("expected records, got {:?}", res.response);
    };
    records
        .into_iter()
        .filter_map(|r| {
            r.as_object()
                .and_then(|o| o.get("properties"))
                .and_then(|p| p.as_object())
                .and_then(|p| p.get("text").cloned())
        })
        .collect()
}

#[test]
fn missing_scope_is_rejected_in_multitenant_mode() {
    let node = multitenant();
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("no-scope".into()),
        context: RequestContext::default(),
        operation: Operation::Query {
            query: Query::Records {
                query: QueryDocument::default(),
            },
        },
    };
    let err = rushdb_server::protocol::ProtocolExecutor::new(&node, None)
        .execute(envelope)
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::MissingScope);

    // single-tenant mode: a scope is rejected instead (scope must be omitted).
    let mut cfg = ServerConfig::default();
    let single = Node::bootstrap(&mut cfg).unwrap();
    let scoped_env = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("scoped".into()),
        context: RequestContext {
            scope: Some("tenant-a".to_string()),
            deadline_ms: None,
            ..Default::default()
        },
        operation: Operation::Query {
            query: Query::Records {
                query: QueryDocument::default(),
            },
        },
    };
    // Single-tenant mode never authenticates a transport scope either.
    let err = rushdb_server::protocol::ProtocolExecutor::new(&single, None)
        .execute(scoped_env)
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ScopeMismatch);
}

#[test]
fn tenant_a_writes_are_invisible_to_tenant_b() {
    let node = multitenant();
    put(&node, "tenant-a", "NOTE", "safe");
    put(&node, "tenant-b", "NOTE", "other");
    let a = find_text(&node, "tenant-a");
    assert_eq!(a, vec![serde_json::json!("safe")]);
    let b = find_text(&node, "tenant-b");

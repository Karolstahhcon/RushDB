//! Protocol v1 native cursor qualification: bounded pages retain one storage
//! snapshot and cannot be used outside their tenant scope.

use rushdb_protocol::{
    Mutation, Operation, PutMode, Query, QueryDocument, RequestContext, RequestEnvelope, Response,
};
use rushdb_server::{Node, ServerConfig};

fn node(multitenant: bool) -> Node {
    let mut config = ServerConfig {
        multitenant,
        ..Default::default()
    };
    Node::bootstrap(&mut config).unwrap()
}

fn put(node: &Node, scope: Option<&str>, rank: u64) {
    rushdb_server::protocol::ProtocolExecutor::new(node, scope.map(str::to_string))
        .execute(RequestEnvelope {
            protocol_version: 1,
            request_id: None,
            context: RequestContext {
                scope: scope.map(str::to_string),
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Mutate {
                mutation: Mutation::PutRecord {
                    mode: PutMode::Create,
                    id: None,
                    label: Some("ITEM".into()),
                    properties: serde_json::json!({ "rank": rank }),
                    vectors: vec![],
                    options: serde_json::json!({}),
                },
            },
        })
        .unwrap();
}

fn ranks(records: &[serde_json::Value]) -> Vec<u64> {
    records
        .iter()
        .map(|record| record["properties"]["rank"].as_u64().expect("numeric rank"))
        .collect()
}

#[test]
fn cursor_pages_are_snapshot_pinned_and_exhaust_once() {
    let node = node(false);
    for rank in 1..=4 {
        put(&node, None, rank);
    }
    let first = rushdb_server::protocol::ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: 1,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Records {
                    query: QueryDocument {
                        labels: vec!["ITEM".into()],
                        order: Some(serde_json::json!({ "rank": "asc" })),
                        limit: Some(2),
                        cursor_ttl_ms: Some(1_000),
                        ..Default::default()
                    },
                },
            },
        })
        .unwrap();
    let Response::Records {
        records,
        total,
        cursor_id,
        ..
    } = first.response
    else {
        panic!("expected records")
    };
    assert_eq!(total, 4);
    assert_eq!(ranks(&records), vec![1, 2]);
    let cursor_id = cursor_id.expect("more snapshot records");

    // A write after page one must not appear in its pinned snapshot.
    put(&node, None, 0);
    let second = rushdb_server::protocol::ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: 1,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Cursor {
                    cursor_id: cursor_id.clone(),
                    limit: 2,
                },
            },
        })
        .unwrap();
    let Response::Records {
        records,
        total,
        cursor_id: next,
        ..
    } = second.response
    else {
        panic!("expected records")
    };
    assert_eq!(total, 4);
    assert_eq!(ranks(&records), vec![3, 4]);
    assert!(next.is_none());

    let error = rushdb_server::protocol::ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: 1,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Cursor {
                    cursor_id,
                    limit: 1,
                },
            },
        })
        .unwrap_err();
    assert_eq!(error.code, rushdb_protocol::ErrorCode::NotFound);
}

#[test]
fn cursor_pages_under_policy_serve_each_allowed_record_once() {
    // Policy-enforced reads intersect allowed labels before scanning, so the
    // cursor pages over the allowed set. This guards the offset advance
    // (`skip + limit`, not the post-filter record count) against re-serving or
    // skipping rows if a future path ever drops records after the scan.
    let node = node(false);
    for rank in 1..=6u64 {
        let label = if rank % 2 == 0 { "ALLOWED" } else { "DENIED" };
        rushdb_server::protocol::ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: 1,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Mutate {
                    mutation: Mutation::PutRecord {
                        mode: PutMode::Create,
                        id: None,
                        label: Some(label.into()),
                        properties: serde_json::json!({ "rank": rank }),
                        vectors: vec![],
                        options: serde_json::json!({}),
                    },
                },
            })
            .unwrap();
    }
    node.scope()
        .security_put_policy(
            &rushdb_authz::DataAccessPolicy {
                allowed_labels: vec!["ALLOWED".into()],
                ..Default::default()
            },
            rushdb_authz::AuthorizationMode::PolicyEnforced,
        )
        .unwrap();

    let first = rushdb_server::protocol::ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: 1,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Records {
                    query: QueryDocument {
                        labels: vec![],
                        order: Some(serde_json::json!({ "rank": "asc" })),
                        limit: Some(1),
                        cursor_ttl_ms: Some(1_000),
                        ..Default::default()
                    },
                },
            },
        })
        .unwrap();
    let Response::Records {
        mut records,
        total,
        mut cursor_id,
        ..
    } = first.response

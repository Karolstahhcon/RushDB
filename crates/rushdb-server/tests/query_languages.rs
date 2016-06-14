//! Comprehensive query-languages test suite.
//!
//! Every enabled language is validated for the full workflow: syntax/read,
//! aggregation, write, delete, and DDL (where applicable). The suite drives the
//! protocol executor directly (the single surface all languages share) with a
//! seeded fixture.

use rushdb_protocol::{
    Describe, ErrorCode, Mutation, Operation, Query, QueryDocument, QueryLanguageCapability,
    RequestContext, RequestEnvelope, Response, ResponseEnvelope, StatementLanguage,
};
use rushdb_server::{Node, ServerConfig};
use serde_json::{json, Value};

fn config() -> ServerConfig {
    ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "test-token".into(),
        data_dir: None,
        multitenant: false,
        capability_secret: None,
        raft: None,
        resources: rushdb_server::resource_config::ResourceConfig::default(),
        transactions: rushdb_server::resource_config::TransactionConfig::default(),
        languages: rushdb_protocol::LanguageSet {
            sql: true,
            gql: true,
            cypher: true,
            gremlin: true,
            sql_pgq: true,
            mongodb: true,
        },
        max_request_bytes: 64 * 1024 * 1024,
        listen_addr: "127.0.0.1".into(),
        tls_cert: None,
        tls_key: None,
        maintenance: false,
        allow_insecure_listen: false,
        require_index_warmup: false,
        request_timeout_ms: 0,
        max_concurrent_requests_per_tenant: 0,
        explicit_flags: std::collections::BTreeSet::new(),
    }
}

fn bootstrap() -> Node {
    let mut cfg = config();
    Node::bootstrap(&mut cfg).unwrap()
}

fn seed(node: &Node) {
    for name in ["Jedi Order", "Sith Order", "Galactic Republic"] {
        node.scope().create_record(
            "FACTION",
            &serde_json::json!({ "name": name, "founded_year_bby": if name == "Jedi Order" { -100 } else { -50 } }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    }
}

fn read(node: &Node, language: StatementLanguage, statement: &str) -> Vec<Value> {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some(format!("read-{language:?}")),
        context: RequestContext::default(),
        operation: Operation::Query {
            query: Query::Statement {
                language,
                statement: statement.to_string(),
                parameters: None,
            },
        },
    };
    let ResponseEnvelope { response, .. } =
        rushdb_server::protocol::ProtocolExecutor::new(node, None)
            .execute(envelope)
            .unwrap_or_else(|e| panic!("{language:?} read failed: {e}"));
    match response {
        Response::Rows { rows, .. } => rows,
        Response::Records { records, .. } => records,
        other => panic!("{language:?} read returned unexpected {other:?}"),
    }
}

fn write(node: &Node, language: StatementLanguage, statement: &str) -> u64 {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some(format!("write-{language:?}")),
        context: RequestContext::default(),
        operation: Operation::Mutate {
            mutation: Mutation::Statement {
                language,
                statement: statement.to_string(),
                parameters: None,
            },
        },
    };
    let ResponseEnvelope { response, .. } =
        rushdb_server::protocol::ProtocolExecutor::new(node, None)
            .execute(envelope)
            .unwrap_or_else(|e| panic!("{language:?} write failed: {e}"));
    match response {
        Response::StatementMutation { affected, .. } => affected,
        other => panic!("{language:?} write returned unexpected {other:?}"),
    }
}

fn describe_languages(node: &Node) -> Vec<QueryLanguageCapability> {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("describe-languages".into()),
        context: RequestContext::default(),
        operation: Operation::Describe {
            describe: Describe::QueryLanguages,
        },
    };
    let ResponseEnvelope { response, .. } =
        rushdb_server::protocol::ProtocolExecutor::new(node, None)
            .execute(envelope)
            .unwrap();
    match response {
        Response::QueryLanguages { languages } => languages,
        other => panic!("unexpected {other:?}"),
    }
}

fn label_count(node: &Node, label: &str) -> usize {
    let lmpg = node.scope().lmpg();
    let snap = lmpg.store.read_view().unwrap();
    rushdb_lmpg::reader::try_access::label_members(&snap, lmpg.tenant, label)
        .unwrap()
        .len()
}

// ── searchquery (native QueryDocument — always enabled) ─────────────────────

#[test]
fn searchquery_read_filter_projection_sort() {
    let node = bootstrap();
    seed(&node);
    let query = json!({
        "labels": ["FACTION"],
        "predicate": { "name": { "$ne": "Sith Order" } },
        "select": { "name": "$record.name" },
        "order": { "name": "asc" }
    });
    let rows = read_searchquery(&node, &query);
    assert_eq!(rows.len(), 2);
    for row in &rows {
        let row = row.as_object().unwrap();
        assert_eq!(row.len(), 1, "projected to `name` only: {row:?}");
        assert_ne!(row["name"], json!("Sith Order"));
    }
}

#[test]
fn searchquery_finite_page_preserves_total_and_order() {
    let node = bootstrap();
    seed(&node);
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("finite-native-page".into()),
        context: RequestContext::default(),
        operation: Operation::Query {
            query: Query::Records {
                query: QueryDocument {
                    labels: vec!["FACTION".into()],
                    order: Some(json!({ "name": "asc" })),
                    limit: Some(2),
                    ..Default::default()
                },
            },
        },
    };
    let response = rushdb_server::protocol::ProtocolExecutor::new(&node, None)
        .execute(envelope)
        .unwrap();
    let Response::Records { records, total, .. } = response.response else {
        panic!("expected a records response");
    };
    assert_eq!(total, 3, "total is pre-pagination");
    assert_eq!(records.len(), 2);
    assert_eq!(
        records
            .iter()
            .map(|record| record["properties"]["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["Galactic Republic", "Jedi Order"]
    );
}

#[test]
fn searchquery_finite_ordered_page_applies_offset_after_global_order() {
    let node = bootstrap();
    seed(&node);
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("finite-native-offset-page".into()),
        context: RequestContext::default(),
        operation: Operation::Query {
            query: Query::Records {
                query: QueryDocument {
                    labels: vec!["FACTION".into()],
                    order: Some(json!({ "name": "asc" })),
                    offset: 1,
                    limit: Some(1),
                    ..Default::default()
                },
            },
        },
    };
    let response = rushdb_server::protocol::ProtocolExecutor::new(&node, None)
        .execute(envelope)
        .unwrap();
    let Response::Records { records, total, .. } = response.response else {
        panic!("expected a records response");
    };
    assert_eq!(total, 3, "total is pre-pagination");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["properties"]["name"], "Jedi Order");
}

#[test]
fn searchquery_aggregate_group_by_and_count() {
    let node = bootstrap();
    seed(&node);
    let query = json!({
        "labels": ["FACTION"],
        "select": { "count": { "$count": "*" } },
        "groupBy": ["$record.name"]
    });
    let rows = read_searchquery(&node, &query);
    assert_eq!(rows.len(), 3, "one group per faction");
    assert!(rows.iter().all(|row| row["count"] == json!(1)), "{rows:?}");
}

#[test]
fn searchquery_write_delete_and_link() {
    let node = bootstrap();
    seed(&node);
    // PutRecord create
    let created = node
        .scope()
        .create_record(
            "FACTION",
            &json!({ "name": "Rebel Alliance" }),
            &[],
            &json!({}),
        )
        .unwrap();
    assert_eq!(
        created.get("label").and_then(Value::as_str),
        Some("FACTION")
    );
    // DeleteRecords via mutation
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Mutate {
            mutation: Mutation::DeleteRecords {
                query: serde_json::from_value(json!({
                    "labels": ["FACTION"],
                    "predicate": { "name": "Rebel Alliance" }
                }))
                .unwrap(),
            },
        },
    };
    let _ = rushdb_server::protocol::ProtocolExecutor::new(&node, None)
        .execute(envelope)
        .unwrap();
    assert_eq!(
        label_count(&node, "FACTION"),
        3,
        "deleted the created record"
    );
}

fn read_searchquery(node: &Node, query: &Value) -> Vec<Value> {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: None,

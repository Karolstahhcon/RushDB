//! HTTP protocol adapter end-to-end tests (raw envelope, scoping, and shaping).
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use rushdb_server::{new_router, ServerConfig};

fn call(
    port: u16,
    path: &str,
    body: &serde_json::Value,
    scope: Option<&str>,
) -> (u16, serde_json::Value) {
    call_with_token(port, path, body, scope, "test-token")
}

fn call_with_token(
    port: u16,
    path: &str,
    body: &serde_json::Value,
    scope: Option<&str>,
    token: &str,
) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let payload = body.to_string();
    let scope_header = scope
        .map(|value| format!("X-RushDB-Tenant: {value}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-RushDB-Token: {token}\r\nContent-Type: application/json\r\n{scope_header}Content-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).unwrap();
    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);
    let response = raw
        .split_once("\r\n\r\n")
        .and_then(|(_, value)| serde_json::from_str(value).ok())
        .unwrap_or(serde_json::Value::Null);
    (status, response)
}

#[test]
fn http_capability_cannot_select_a_different_tenant() {
    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";
    let cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "boot-token".into(),
        data_dir: None,
        multitenant: true,
        capability_secret: Some(SECRET.to_vec()),
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
    };
    let (mut router, port) = new_router(cfg).expect("bootstrap");
    std::thread::spawn(move || router.serve_forever());
    std::thread::sleep(Duration::from_millis(40));
    let token = rushdb_server::capability::issue_for_tenant(
        SECRET,
        "service-a",
        "write",
        Some("tenant-a"),
        60,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );

    let request = put_request("tenant-a", "NOTE", &serde_json::json!({ "text": "safe" }));
    let (accepted, _) = call_with_token(
        port,
        "/protocol/v1/execute",
        &request,
        Some("tenant-a"),
        &token,
    );
    assert_eq!(accepted, 200);

    let cross_tenant = put_request("tenant-b", "NOTE", &serde_json::json!({ "text": "leak" }));
    let (rejected, response) = call_with_token(
        port,
        "/protocol/v1/execute",
        &cross_tenant,
        Some("tenant-b"),
        &token,
    );
    assert_eq!(rejected, 401, "{response}");
}

#[test]
fn tenant_capability_cannot_export_another_tenant() {
    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";
    let cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "boot-token".into(),
        data_dir: None,
        multitenant: true,
        capability_secret: Some(SECRET.to_vec()),
        raft: None,
        resources: rushdb_server::resource_config::ResourceConfig::default(),
        transactions: rushdb_server::resource_config::TransactionConfig::default(),
        languages: rushdb_protocol::LanguageSet::default(),
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
    };
    let (mut router, port) = new_router(cfg).expect("bootstrap");
    std::thread::spawn(move || router.serve_forever());
    std::thread::sleep(Duration::from_millis(40));

    for (scope, text) in [("tenant-a", "a-secret"), ("tenant-b", "b-secret")] {
        let (status, response) = call_with_token(
            port,
            "/protocol/v1/execute",
            &put_request(scope, "NOTE", &serde_json::json!({ "text": text })),
            Some(scope),
            "boot-token",
        );
        assert_eq!(status, 200, "{response}");
    }

    let token = rushdb_server::capability::issue_for_tenant(
        SECRET,
        "service-a",
        "read",
        Some("tenant-a"),
        60,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );
    let cross_tenant_backup = serde_json::json!({
        "protocolVersion": 1,
        "context": { "scope": "tenant-b" },
        "operation": { "kind": "backup", "request": { "scopes": ["tenant-b"] } }
    });
    let (status, response) = call_with_token(
        port,
        "/protocol/v1/execute",
        &cross_tenant_backup,
        Some("tenant-b"),
        &token,
    );
    assert_eq!(status, 401, "{response}");

    let own_backup = serde_json::json!({
        "protocolVersion": 1,
        "context": { "scope": "tenant-a" },
        "operation": { "kind": "backup", "request": { "scopes": [] } }
    });
    let (status, response) = call_with_token(
        port,
        "/protocol/v1/execute",
        &own_backup,
        Some("tenant-a"),
        &token,
    );
    assert_eq!(status, 200, "{response}");
    let backup = response["response"]["tenants"][0]["jsonl"]
        .as_str()
        .expect("backup JSONL");
    assert!(backup.contains("a-secret"));
    assert!(!backup.contains("b-secret"));
}

fn join_request(scope: &str) -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": 1,
        "requestId": "http-join-1",
        "context": { "scope": scope },
        "operation": {
            "kind": "query",
            "query": {
                "kind": "joinPairs",
                "left": { "label": "SOURCE", "filter": {} },
                "right": { "label": "TARGET", "filter": {} },
                "predicate": {
                    "kind": "propertyOverlap",
                    "leftProperty": "targetId",
                    "rightProperty": "id"
                },
                "excludeSameRecord": false,
                "projection": "count",
                "limit": 100
            }
        }
    })
}

fn put_request(scope: &str, label: &str, properties: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": 1,
        "context": { "scope": scope },
        "operation": {
            "kind": "mutate",
            "mutation": {
                "kind": "putRecord",
                "mode": "create",
                "label": label,
                "properties": properties,
                "options": {}
            }
        }
    })
}

fn records_request(scope: &str, label: &str) -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": 1,
        "context": { "scope": scope },
        "operation": {
            "kind": "query",
            "query": {
                "kind": "records",
                "query": {
                    "labels": [label],
                    "predicate": {},
                    "offset": 0,
                    "limit": 100
                }
            }
        }
    })
}

fn ingest_request_with_options(
    documents: &serde_json::Value,
    options: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": 1,
        "context": {},
        "operation": {
            "kind": "ingest",
            "ingest": {
                "kind": "documents",
                "documents": documents,
                "options": options
            }
        }
    })
}

fn ingest_request(documents: &serde_json::Value) -> serde_json::Value {
    ingest_request_with_options(documents, &serde_json::json!({ "returnResult": true }))
}

fn labels_request() -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": 1,
        "context": {},
        "operation": {
            "kind": "describe",
            "describe": {
                "kind": "labels",
                "query": {
                    "labels": [],
                    "predicate": {},
                    "offset": 0
                }
            }
        }
    })
}

fn statement_mutation(language: &str, statement: &str) -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": 1,
        "context": {},
        "operation": {
            "kind": "mutate",
            "mutation": {
                "kind": "statement",
                "language": language,
                "statement": statement
            }
        }
    })
}

#[test]
fn http_carries_native_snapshot_cursor_envelopes() {
    let cfg = ServerConfig {
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
        languages: rushdb_protocol::LanguageSet::default(),
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
    };
    let (mut router, port) = new_router(cfg).expect("bootstrap");
    std::thread::spawn(move || router.serve_forever());
    std::thread::sleep(Duration::from_millis(40));
    for (id, rank) in [("h1", 1), ("h2", 2), ("h3", 3)] {
        let request = serde_json::json!({
            "protocolVersion": 1, "context": {}, "operation": { "kind": "mutate", "mutation": {
                "kind": "putRecord", "mode": "create", "label": "CURSOR_ITEM",
                "properties": {"name": id, "rank": rank}, "options": {}
            }}
        });
        assert_eq!(call(port, "/protocol/v1/execute", &request, None).0, 200);
    }
    let first = serde_json::json!({
        "protocolVersion": 1, "context": {}, "operation": { "kind": "query", "query": {
            "kind": "records", "query": {"labels":["CURSOR_ITEM"], "predicate":{}, "order":{"rank":"asc"}, "limit":2, "cursorTtlMs":1000}
        }}
    });
    let (status, first) = call(port, "/protocol/v1/execute", &first, None);
    assert_eq!(status, 200, "{first}");
    let cursor = first["response"]["cursorId"].as_str().unwrap().to_string();
    assert_eq!(first["response"]["records"].as_array().unwrap().len(), 2);
    let next = serde_json::json!({
        "protocolVersion": 1, "context": {}, "operation": { "kind": "query", "query": {
            "kind": "cursor", "cursorId": cursor, "limit": 2
        }}
    });
    let (status, next) = call(port, "/protocol/v1/execute", &next, None);
    assert_eq!(status, 200, "{next}");
    assert_eq!(next["response"]["total"], 3);
    assert_eq!(next["response"]["records"].as_array().unwrap().len(), 1);
    assert!(next["response"]["cursorId"].is_null());
}

#[test]
fn http_executes_all_writable_query_language_profiles() {
    let cfg = ServerConfig {
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
    };
    let (mut router, port) = new_router(cfg).expect("bootstrap");
    std::thread::spawn(move || router.serve_forever());
    std::thread::sleep(Duration::from_millis(40));

    for (language, statement) in [
        ("cypher", "CREATE (:Person {name: 'Cypher'})"),
        ("gql", "INSERT (:Person {name: 'GQL'})"),
        ("gremlin", "g.addV('Person').property('name', 'Gremlin')"),
    ] {
        let (status, response) = call(
            port,
            "/protocol/v1/execute",
            &statement_mutation(language, statement),
            None,
        );
        assert_eq!(status, 200, "{language}: {response}");
        assert_eq!(response["response"]["kind"], "statementMutation");
        assert!(response["response"]["affected"].as_u64().unwrap() > 0);
    }

    let (status, response) = call(
        port,
        "/protocol/v1/execute",
        &statement_mutation("sql", "INSERT INTO person (name) VALUES ('DataFusion')"),
        None,
    );
    assert_eq!(status, 200, "sql: {response}");
    assert_eq!(response["response"]["kind"], "statementMutation");
    assert_eq!(response["response"]["affected"], 1);

    let query = serde_json::json!({
        "protocolVersion": 1,
        "context": {},
        "operation": {
            "kind": "query",
            "query": {
                "kind": "statement",
                "language": "sql",
                "statement": "SELECT name FROM Person ORDER BY name"
            }
        }
    });
    let (status, response) = call(port, "/protocol/v1/execute", &query, None);
    assert_eq!(status, 200, "{response}");
    assert_eq!(response["response"]["total"], 4);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
)]
fn protocol_http_preserves_raw_envelope_and_fails_closed_on_scope_mismatch() {
    let cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "test-token".into(),
        data_dir: None,
        multitenant: true,
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

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

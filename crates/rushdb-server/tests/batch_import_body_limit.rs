//! Large batch imports must not be rejected by a too-small request-body limit.
//!
//! The `Bytes` extractor is gated by axum's `DefaultBodyLimit` (2MB default),
//! which is independent of the tower-http `RequestBodyLimitLayer` (64MB). A
//! ~3MB batch import previously returned 413 "length limit exceeded" despite
//! the intended 64MB budget.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use rushdb_server::{new_router, ServerConfig};

#[test]
fn batch_import_over_2mb_is_accepted() {
    let cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "t".into(),
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
    std::thread::sleep(Duration::from_millis(50));

    // >2MB (past axum's default) and comfortably under the 64MB budget.
    let mut rows: Vec<serde_json::Value> = Vec::new();
    for i in 0..15000 {
        rows.push(serde_json::json!({
            "id": format!("r{i}"),
            "name": format!("record {i}"),
            "blob": "x".repeat(100)
        }));
    }
    let envelope = serde_json::json!({
        "protocolVersion": 1,
        "context": {},
        "operation": { "kind": "ingest", "ingest": { "kind": "documents", "label": "BIG", "documents": rows, "options": {} } }
    });
    let body = envelope.to_string();
    assert!(
        body.len() > 2 * 1024 * 1024,
        "payload must exceed axum's 2MB default"
    );

    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    let request = format!(
        "POST /protocol/v1/execute HTTP/1.1\r\nHost: localhost\r\nX-RushDB-Token: t\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(request.as_bytes());
    let _ = stream.flush();
    let mut raw = String::new();
    let _ = stream.read_to_string(&mut raw);
    let status_line = raw.lines().next().unwrap_or("(no response)").to_string();
    assert!(
        !status_line.contains("413"),
        "batch import must not 413: {status_line} {raw}"
    );

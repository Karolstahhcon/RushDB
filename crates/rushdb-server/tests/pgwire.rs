//! PostgreSQL wire-protocol adapter end-to-end tests.
use rushdb_protocol::LanguageSet;
use rushdb_server::{new_gateway, ServerConfig};
use std::io::{Read, Write};
use std::net::TcpStream;
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

fn start(multitenant: bool, capability_secret: Option<&[u8]>) -> u16 {
    start_gateway(multitenant, capability_secret, false).1
}

fn start_gateway(
    multitenant: bool,
    capability_secret: Option<&[u8]>,
    serve_http: bool,
) -> (u16, u16) {
    let config = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "pgwire-token".into(),
        data_dir: None,
        multitenant,
        capability_secret: capability_secret.map(<[u8]>::to_vec),
        raft: None,
        resources: rushdb_server::resource_config::ResourceConfig::default(),
        transactions: rushdb_server::resource_config::TransactionConfig::default(),
        languages: LanguageSet {
            sql: true,
            ..Default::default()
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
    let (mut http, http_port, mut pgwire, pgwire_port, grpc, grpc_port, bolt, bolt_port) =
        new_gateway(config, 0).expect("gateway bootstrap");
    if serve_http {
        std::thread::spawn(move || http.serve_forever());
    } else {
        drop(http);
    }
    std::thread::spawn(move || pgwire.serve_forever());
    let _ = (grpc, grpc_port, bolt, bolt_port);
    (http_port, pgwire_port)
}

fn start_gateway_timeout(request_timeout_ms: u64) -> (u16, u16) {
    let config = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "pgwire-token".into(),
        data_dir: None,
        multitenant: false,
        capability_secret: None,
        raft: None,
        resources: rushdb_server::resource_config::ResourceConfig::default(),
        transactions: rushdb_server::resource_config::TransactionConfig::default(),
        languages: LanguageSet {
            sql: true,
            ..Default::default()
        },
        max_request_bytes: 64 * 1024 * 1024,
        listen_addr: "127.0.0.1".into(),
        tls_cert: None,
        tls_key: None,
        maintenance: false,
        allow_insecure_listen: false,
        require_index_warmup: false,
        request_timeout_ms,
        max_concurrent_requests_per_tenant: 0,
        explicit_flags: std::collections::BTreeSet::new(),
    };
    let (mut router, http_port, mut pg, pg_port, _grpc, _, _bolt, _) =
        rushdb_server::new_gateway(config, 0).expect("gateway");
    std::thread::spawn(move || router.serve_forever());
    std::thread::spawn(move || pg.serve_forever());
    std::thread::sleep(std::time::Duration::from_millis(60));
    (http_port, pg_port)
}

fn put_record(http_port: u16, scope: Option<&str>, label: &str, properties: &serde_json::Value) {
    let body = serde_json::json!({
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
    .to_string();
    let mut stream = TcpStream::connect(("127.0.0.1", http_port)).unwrap();
    let scope_header = scope
        .map(|scope| format!("X-RushDB-Tenant: {scope}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "POST /protocol/v1/execute HTTP/1.1\r\nHost: localhost\r\nX-RushDB-Token: pgwire-token\r\nContent-Type: application/json\r\n{scope_header}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
}

async fn connect(
    port: u16,
    database: &str,
    password: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("rushdb")
        .dbname(database)
        .password(password);
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

#[tokio::test]
async fn standard_client_runs_simple_and_extended_queries() {
    let (http_port, port) = start_gateway(false, None, true);
    put_record(
        http_port,
        None,
        "NOTE",
        &serde_json::json!({ "name": "alpha", "position": 7, "active": true }),
    );
    let client = connect(port, "rushdb", "pgwire-token").await.unwrap();

    let messages = client.simple_query("SELECT 1;").await.unwrap();
    let row = messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(row.get(0), Some("1"));

    // Columns come back in select-list order, not sorted by name.
    let messages = client
        .simple_query("SELECT position, name, active FROM NOTE")
        .await
        .unwrap();
    let row = messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .unwrap();
    let names: Vec<&str> = row
        .columns()

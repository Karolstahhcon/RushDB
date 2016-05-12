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

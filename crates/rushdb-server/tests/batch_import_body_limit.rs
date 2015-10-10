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

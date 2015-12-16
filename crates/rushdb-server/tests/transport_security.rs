//! 7A — transport security guard: cleartext transports (pgwire/gRPC/Bolt) must
//! not bind a non-loopback `--listen` address unless the operator explicitly
//! acknowledges it with `--allow-insecure-listen`.

use std::sync::atomic::{AtomicU16, Ordering};

use rushdb_server::{new_gateway, new_router, ServerConfig};

static GRPC_PORT: AtomicU16 = AtomicU16::new(39040);

fn cfg(listen: &str, cleartext: bool, allow: bool) -> ServerConfig {
    ServerConfig {
        port: 0,
        listen_addr: listen.to_string(),
        pgwire_port: 0,
        grpc_port: if cleartext {
            GRPC_PORT.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        },
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
        tls_cert: None,
        tls_key: None,
        maintenance: false,
        allow_insecure_listen: allow,
        require_index_warmup: false,
        request_timeout_ms: 0,
        max_concurrent_requests_per_tenant: 0,
        explicit_flags: std::collections::BTreeSet::new(),
    }
}

#[test]
fn non_loopback_cleartext_bind_is_refused_without_acknowledgement() {
    let cfg = cfg("0.0.0.0", true, false);
    let Err(err) = new_gateway(cfg, 0) else {
        panic!("expected refusal")
    };
    assert!(
        err.contains("cleartext") && err.contains("allow-insecure-listen"),
        "unexpected error: {err}"
    );
}

fn bind_ok(cfg: ServerConfig) {
    bind_ok_on(cfg, 0);
}

fn bind_ok_on(cfg: ServerConfig, pgwire_port: u16) {
    let (_router, _hp, _pg, _pp, grpc, _gp, _bolt, _bp) =
        new_gateway(cfg, pgwire_port).unwrap_or_else(|e| panic!("must bind: {e}"));
    drop(grpc);
}

#[test]
fn allow_insecure_listen_permits_non_loopback_bind() {
    bind_ok(cfg("0.0.0.0", true, true));
}

#[test]
fn loopback_cleartext_bind_needs_no_acknowledgement() {
    bind_ok(cfg("127.0.0.1", true, false));
}

#[test]
fn multitenant_cleartext_transport_cannot_bind_externally_even_with_acknowledgement() {
    let mut config = cfg("0.0.0.0", true, true);
    config.multitenant = true;
    let Err(error) = new_gateway(config, 0) else {
        panic!("must refuse global-token multitenant transport")
    };

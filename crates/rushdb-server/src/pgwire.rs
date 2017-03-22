//! PostgreSQL adapter backed by the standard `pgwire` crate.
use crate::{http::HttpRouter, node::Node, RequestScope, ServerConfig};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures::stream;
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;
use rushdb_protocol::{
    ErrorCode, Operation, ProtocolError, Query, RequestContext, RequestEnvelope,
    Response as RushResponse, PROTOCOL_VERSION,
};
use serde_json::{Map, Value};
use std::{fmt::Debug, net::TcpListener, sync::Arc};

mod settings;

/// A bound PostgreSQL wire-protocol adapter, or a disabled placeholder when the
/// `sql` statement language is off.
pub struct PgWireServer {
    cfg: Option<ServerConfig>,
    node: Option<Arc<Node>>,
    listener: Option<TcpListener>,
}
impl PgWireServer {
    /// A server that does not bind (the `sql` language is disabled at startup).
    #[must_use]
    pub fn disabled() -> Self {
        PgWireServer {
            cfg: None,
            node: None,
            listener: None,
        }
    }

    fn bind(cfg: ServerConfig, node: Arc<Node>, port: u16) -> Result<(Self, u16), String> {
        let l = TcpListener::bind((cfg.listen_addr.as_str(), port)).map_err(|e| e.to_string())?;
        let p = l.local_addr().map_err(|e| e.to_string())?.port();
        l.set_nonblocking(true).map_err(|e| e.to_string())?;
        Ok((
            Self {
                cfg: Some(cfg),
                node: Some(node),
                listener: Some(l),
            },
            p,
        ))
    }
    /// Accept pgwire connections until the listener is dropped or fails to be
    /// adopted by Tokio. Returns immediately when the server is disabled.
    ///
    /// # Panics
    ///
    /// Panics if an enabled server lacks its config/node (an internal
    /// invariant), if the Tokio runtime cannot be built, or if the pre-bound
    /// listener cannot be adopted by Tokio.
    /// # Errors
    /// Returns an error when sessions do not finish within the drain deadline.
    pub fn serve_forever(&mut self) -> Result<(), String> {
        let Some(l) = self.listener.take() else {
            // sql language disabled: nothing to serve.
            return Ok(());
        };
        let cfg = self.cfg.clone().expect("enabled pgwire has config");
        let node = self.node.clone().expect("enabled pgwire has node");
        let h = Handlers::new(cfg, Arc::clone(&node));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let outcome = rt.block_on(async move {
            let l = tokio::net::TcpListener::from_std(l).unwrap();
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = l.accept() => {
                        let (s, _) = accepted.map_err(|error| format!("pgwire accept failed: {error}"))?;
                        // C7: bound live sessions; refuse (close) beyond the cap
                        // instead of accepting unbounded connections.
                        if connections.len() >= MAX_PGWIRE_CONNECTIONS {
                            tracing::warn!(limit = MAX_PGWIRE_CONNECTIONS, "pgwire connection limit reached; refusing");
                            drop(s);
                            continue;
                        }
                        connections.spawn(process_socket(s, None, h.clone()));
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                    () = tokio::time::sleep(std::time::Duration::from_millis(25)) => {
                        if node.store().maintenance_aborted() {
                            tracing::info!("pgwire transport draining after store shutdown");
                            break;
                        }
                    }
                }
            }
            let drain = async { while connections.join_next().await.is_some() {} };
            if tokio::time::timeout(std::time::Duration::from_secs(5), drain)
                .await
                .is_err()
            {
                tracing::warn!("pgwire transport drain deadline elapsed");
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                return Err("pgwire shutdown drain deadline elapsed".into());
            }
            Ok(())
        });
        rt.shutdown_timeout(std::time::Duration::from_secs(1));
        outcome
    }
}
/// True when `listen_addr` is a loopback address (127.0.0.1, `::1`, or
/// localhost); only loopback binds are safe for cleartext transports.
pub(crate) fn is_loopback(addr: &str) -> bool {
    matches!(addr, "127.0.0.1" | "::1" | "localhost") || addr.starts_with("127.")
}

/// Refuse to expose cleartext transports (pgwire/gRPC/Bolt) on a non-loopback
/// `--listen` address unless the operator explicitly acknowledges it via
/// `--allow-insecure-listen`. Loopback binds and HTTP-with-TLS are unaffected.
/// Checked on the effective configuration (after `config.yml` is applied),
/// counting only the transports that will actually bind.
fn guard_insecure_listen(cfg: &ServerConfig) -> Result<(), String> {
    let pgwire_enabled = cfg.pgwire_port != 0 && cfg.languages.sql;
    let cleartext_enabled = pgwire_enabled || cfg.grpc_port != 0 || crate::bolt::bolt_enabled(cfg);
    // HTTP has signed, tenant-bound capabilities. pgwire, gRPC, and Bolt
    // currently authenticate only the global boot token, so exposing any of
    // them directly on a multitenant non-loopback listener would let a holder
    // select an arbitrary tenant scope. They remain safe behind a local trusted
    // proxy, or for a single-tenant server. `--allow-insecure-listen`
    // acknowledges cleartext; it must not disable this tenancy guard.
    if cfg.multitenant && cleartext_enabled && !is_loopback(&cfg.listen_addr) {
        return Err(
            "refusing to bind pgwire/gRPC/Bolt on a non-loopback multitenant listener: these transports require tenant-bound credentials or a trusted local proxy".into(),
        );
    }
    if cleartext_enabled && !is_loopback(&cfg.listen_addr) && !cfg.allow_insecure_listen {
        return Err(format!(
            "refusing to bind pgwire/gRPC/Bolt (cleartext) to non-loopback listen address `{}` without --allow-insecure-listen; keep --listen loopback or add the explicit flag",
            cfg.listen_addr
        ));
    }
    Ok(())
}

type Gateway = (
    HttpRouter,
    u16,
    PgWireServer,
    u16,
    crate::GrpcServer,
    u16,
    crate::BoltServer,
    u16,
);

/// Maximum concurrent pgwire sessions (C7, cloud-capacity F8).
const MAX_PGWIRE_CONNECTIONS: usize = 1024;

/// Boot the node and bind every enabled transport (HTTP, pgwire, gRPC, Bolt).
///
/// # Errors
///
/// Returns a message when the listen security guard refuses the bind, the node
/// cannot bootstrap, or any enabled transport cannot bind its address.
pub fn new_gateway(cfg: ServerConfig, port: u16) -> Result<Gateway, String> {
    new_gateway_with_boot_listener(cfg, port, None)
}

/// [`new_gateway`] that adopts the socket a [`crate::boot::BootListener`]
/// served probes on during bootstrap.
///
/// # Errors
/// Returns a message when the node cannot bootstrap or a transport cannot bind.
pub fn new_gateway_with_boot_listener(
    mut cfg: ServerConfig,
    port: u16,
    boot_listener: Option<crate::boot::BootListener>,
) -> Result<Gateway, String> {
    // CLI/env pgwire port is the fallback; a stored config.yml may override it.
    cfg.pgwire_port = port;
    let node = Arc::new(Node::bootstrap(&mut cfg)?);
    guard_insecure_listen(&cfg)?;
    let (http, http_port) = match boot_listener {
        Some(listener) => {
            crate::http::bind_router_with_listener(cfg.clone(), node.clone(), listener.finish()?)?
        }
        None => crate::http::bind_router(cfg.clone(), node.clone())?,
    };
    let (pgwire, pgwire_port) = if cfg.languages.sql {
        PgWireServer::bind(cfg.clone(), node.clone(), cfg.pgwire_port)?
    } else {
        // `sql` language disabled: do not bind the PostgreSQL wire protocol.
        (PgWireServer::disabled(), 0)
    };
    let (grpc, grpc_port) = crate::grpc::bind_grpc(&cfg, node.clone())?;
    let (bolt, bolt_port) = crate::bolt::bind_bolt(&cfg, node)?;
    Ok((
        http,
        http_port,
        pgwire,
        pgwire_port,
        grpc,
        grpc_port,
        bolt,
        bolt_port,
    ))
}

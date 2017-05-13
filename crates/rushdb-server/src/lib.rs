//! Pgwire and HTTP surfaces for the `RushDB` engine (`rushdb serve`) plus the
//! one-command distribution entry (`rushdb up`) — deployment slice of plan
//! documents 04/10/25. The server hosts SQL over PostgreSQL wire, the
//! versioned HTTP database protocol, and operational endpoints.

use thiserror::Error;

pub mod allocator;
pub mod archive;
pub mod bolt;
pub mod boot;
mod bulk_share;
pub mod capability;
pub mod config;
pub mod grpc;
pub mod http;
pub mod lifecycle;
mod node;
pub mod pgwire;
pub mod protocol;
mod protocol_http;
pub mod replication;
pub mod resource_config;
pub mod retention;

pub use bolt::bind_bolt;
pub use grpc::{bind_grpc, NodeEnvelopeHandler};
pub use http::{new_router, HttpRouter};
pub use node::scope::RequestScope;
pub use node::Node;
pub use pgwire::{new_gateway, PgWireServer};
pub use rushdb_bolt::BoltServer;
pub use rushdb_grpc::GrpcServer;

/// Effective boot configuration for a `rushdb serve`/`rushdb up` process.
#[derive(Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent operator switches (multitenancy, maintenance, TLS listen, index-warmup readiness) are clearer as named fields than as a bitflag set"
)]
pub struct ServerConfig {
    /// HTTP protocol adapter port.
    pub port: u16,
    /// Bind address for the HTTP protocol adapter (default `127.0.0.1`).
    /// Set to `0.0.0.0` to expose the engine beyond loopback (pair with TLS).
    pub listen_addr: String,
    /// Path to a PEM certificate chain for HTTPS. Both `tls_cert` and
    /// `tls_key` must be set to enable TLS on the HTTP listener.
    pub tls_cert: Option<std::path::PathBuf>,
    /// Path to a PEM private key (PKCS#8) for HTTPS.
    pub tls_key: Option<std::path::PathBuf>,
    /// PostgreSQL wire-protocol port.
    pub pgwire_port: u16,
    /// gRPC transport port (`0` = disabled).
    pub grpc_port: u16,
    /// Bolt (Neo4j wire) transport port (`0` = disabled; requires a graph
    /// language).
    pub bolt_port: u16,
    /// Boot bearer token granting full access on every transport.
    pub token: String,
    /// Directory holding the durable store and `config.yml`; `None` is an
    /// in-memory node.
    pub data_dir: Option<std::path::PathBuf>,
    /// Enables scope-keyed multi-tenancy when set. Each request must present a
    /// scope (`X-RushDB-Tenant` on HTTP, `database` on pgwire); the tenant
    /// identity is the full SHA-256 of that scope, so the keyspace prefix is
    /// the isolation boundary and no discriminator property is ever stored on
    /// records. `false` is true single-tenant mode: no scope, one keyspace.
    pub multitenant: bool,
    /// Optional HMAC secret for signed capability tokens. When set, a valid
    /// capability presented via `X-RushDB-Token` is accepted (scope-gated);
    /// the boot `token` always remains a full-access fallback.
    pub capability_secret: Option<Vec<u8>>,
    /// Optional raft replication: when `Some`, the node boots an in-process
    /// `OpenRaft` cluster of `size` nodes and routes every local commit through

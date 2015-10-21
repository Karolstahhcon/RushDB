//! gRPC transport adapter (server side).
//!
//! Implements `rushdb_grpc::EnvelopeHandler` on top of `ProtocolExecutor`,
//! applying the tenant scope thread-local exactly like the HTTP adapter, and
//! binds a `GrpcServer` when `grpc-port` is configured (mirroring the pgwire
//! adapter's disabled/bind/thread pattern).

use std::sync::Arc;

use rushdb_grpc::{EnvelopeHandler, GrpcServer};
use rushdb_protocol::{ProtocolError, RequestEnvelope, ResponseEnvelope};

use crate::protocol::scheduler_error_to_protocol_error;
use crate::{Node, ServerConfig};

/// Executes canonical envelopes on a node under the request tenant scope.
#[derive(Clone)]
pub struct NodeEnvelopeHandler {
    node: Arc<Node>,
}

impl NodeEnvelopeHandler {
    /// Wrap a booted node as an envelope handler for the gRPC/Bolt transports.
    pub fn new(node: Arc<Node>) -> Self {
        NodeEnvelopeHandler { node }
    }
}

impl EnvelopeHandler for NodeEnvelopeHandler {
    fn execute(
        &self,
        envelope: RequestEnvelope,
        scope: Option<String>,
    ) -> Result<ResponseEnvelope, ProtocolError> {
        // A single-tenant node has one keyspace: a transport scope (gRPC
        // `x-rushdb-tenant`, Bolt HELLO `db`) is refused like the HTTP header,
        // never served as a separate keyspace.
        if scope.is_some() && !self.node.multitenant() {
            return Err(ProtocolError::new(
                rushdb_protocol::ErrorCode::ScopeMismatch,
                "tenant scope must be omitted in single-tenant mode",
            ));
        }
        let scope_for_job = scope.clone();
        let tenant = self.node.schedule_key(scope.as_deref());
        let started = std::time::Instant::now();
        let node = Arc::clone(&self.node);

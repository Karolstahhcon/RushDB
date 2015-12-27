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

//! Bolt (Neo4j wire) transport adapter (server side).
//!
//! Binds a `BoltServer` when `bolt-port` is configured AND `cypher` is
//! enabled — Bolt sends every statement as openCypher. Mirrors the gRPC
//! adapter: `disabled()` when not bound.

use std::sync::Arc;

use rushdb_bolt::BoltServer;
use rushdb_protocol::EnvelopeHandler;

use crate::{Node, ServerConfig};

/// Whether this configuration serves Bolt: a port and the `cypher` language.
pub(crate) fn bolt_enabled(cfg: &ServerConfig) -> bool {
    cfg.bolt_port != 0 && cfg.languages.cypher

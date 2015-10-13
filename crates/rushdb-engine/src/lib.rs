//! Thin in-process facade over storage, LMPG, and the query executor.
//!
//! [`Engine`] is a convenience surface for embedding a single store without
//! the transport layer. It is deliberately narrower than the protocol
//! executor in `rushdb-server`: it exposes document import, search, and
//! aggregation, not the full mutation/describe/index/transaction surface.
use std::sync::Arc;

use rushdb_lmpg::{Lmpg, LmpgError};
use rushdb_query::{Page, QueryError, SearchRequest};
use rushdb_types::ids::TenantId;

pub mod context;
pub mod scheduler;
pub use context::{ExecutionContext, ExecutionError, OperationClass};
pub use scheduler::{BoundedScheduler, SchedulerError};

/// Storage engine boundary. An optional opaque scope is mapped to an internal
/// tenant before any lookup or planning. Application resource identifiers do
/// not cross this boundary.
pub struct Engine {
    /// Shared storage backing this engine and its tenant facades.
    pub store: Arc<rushdb_storage::Store>,
    scheduler: Arc<BoundedScheduler>,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new_in_memory()
    }
}

fn response_bytes<T: serde::Serialize>(value: &T) -> Result<usize, QueryError> {
    serde_json::to_vec(value)
        .map(|encoded| encoded.len())
        .map_err(|error| {
            QueryError::ResourceLimit(format!("response serialization failed: {error}"))
        })
}

/// Estimate the memory needed to lower a JSON document without allocating.
/// Import staging retains the input tree, lowered scalar copies, and transaction
/// operations, so admission reserves a conservative multiple before cloning it.
fn import_memory_estimate(value: &serde_json::Value) -> usize {
    import_memory_estimate_at_depth(value, 0)
}

fn import_memory_estimate_at_depth(value: &serde_json::Value, depth: usize) -> usize {
    const NODE_OVERHEAD: usize = 32;
    const PROPERTY_METADATA: usize = 64;
    let (payload, children) = match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            (0, 0)
        }
        serde_json::Value::String(value) => (value.len(), 0),
        serde_json::Value::Array(values) => (
            0,
            values.iter().fold(0usize, |total, value| {
                total.saturating_add(import_memory_estimate_at_depth(
                    value,
                    depth.saturating_add(1),
                ))
            }),
        ),
        serde_json::Value::Object(values) => (
            values.iter().fold(0usize, |total, (key, _)| {
                total
                    .saturating_add(key.len())
                    .saturating_add(PROPERTY_METADATA)
            }),
            values.iter().fold(0usize, |total, (_, value)| {
                total.saturating_add(import_memory_estimate_at_depth(
                    value,
                    depth.saturating_add(1),
                ))
            }),
        ),
    };

    // Each node is retained by the input tree and copied into staging. Weight
    // the node itself by its depth, then add child costs once. Multiplying a
    // recursively estimated subtree by depth at every ancestor compounds the
    // factors and makes a narrow document appear factorially large.
    NODE_OVERHEAD
        .saturating_add(payload)
        .saturating_mul(depth.saturating_add(1))
        .saturating_add(children)
}

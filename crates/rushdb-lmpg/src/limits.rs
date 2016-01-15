//! The bounds one request's reads run under.
//!
//! Like PostgreSQL and Neo4j, a filter without a usable index runs as a walk
//! of the records it names. What bounds it is the server's ceilings
//! (`request-timeout-ms`, `query-memory-max`, `unindexed-walk-max-owners`,
//! each `0` = none) and, like `CockroachDB`'s session settings, a request's own
//! values, which may only tighten them: a request cannot lift a ceiling the
//! operator set.

use std::time::Instant;

use rushdb_storage::resources::StorageResources;

/// A request's own bounds. Each `None` keeps the server's ceiling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestLimits {
    /// Refuse a filter without a usable index that would check more records
    /// than this; `Some(0)` refuses every such walk (`CockroachDB`'s
    /// `disallow_full_table_scans` with `large_full_scan_rows = 0`).
    pub unindexed_walk_max_owners: Option<u64>,
    /// Working-set cap of one query, in bytes.
    pub query_memory_max_bytes: Option<u64>,
    /// When the request's time is up.
    pub deadline: Option<Instant>,
}

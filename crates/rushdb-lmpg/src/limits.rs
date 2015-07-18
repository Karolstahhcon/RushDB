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

/// The bounds in force: the server's ceilings tightened by the request's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueryLimits {
    unindexed_walk_max_owners: Option<u64>,
    query_memory_max_bytes: Option<u64>,
    deadline: Option<Instant>,
}

impl QueryLimits {
    /// The server's ceilings alone (`0` = none).
    #[must_use]
    pub fn of_server(config: &StorageResources) -> Self {
        let set = |value: u64| (value != 0).then_some(value);
        Self {
            unindexed_walk_max_owners: set(config.unindexed_walk_max_owners),
            query_memory_max_bytes: set(config.query_memory_max_bytes),
            deadline: None,
        }
    }

    /// These bounds tightened by a request's own.
    #[must_use]
    pub fn tightened(self, request: RequestLimits) -> Self {
        Self {
            unindexed_walk_max_owners: tighter(
                self.unindexed_walk_max_owners,
                request.unindexed_walk_max_owners,
            ),
            query_memory_max_bytes: tighter(
                self.query_memory_max_bytes,
                request.query_memory_max_bytes,
            ),
            deadline: tighter(self.deadline, request.deadline),
        }
    }

    /// The most records a walk without a usable index may check.
    #[must_use]
    pub fn unindexed_walk_max_owners(&self) -> Option<u64> {
        self.unindexed_walk_max_owners
    }

    /// The working-set cap of one query, in bytes.
    #[must_use]
    pub fn query_memory_max_bytes(&self) -> Option<u64> {
        self.query_memory_max_bytes
    }

    /// Whether the request's time is up.
    #[must_use]
    pub fn expired(&self) -> bool {

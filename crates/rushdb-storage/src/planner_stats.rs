//! Evictable planner-statistics cache (DC1 "on demand, evict when
//! idle").
//!
//! Value distributions are loaded on a planner miss by walking the value
//! dictionary (`rushdb-query`'s `stats_cache`), kept while used, and dropped
//! after an idle TTL, when the byte budget needs room (LRU), or once the
//! data has moved `max_age_commits` past the entry. Nothing here is ever a
//! query result: exact answers read counters at the snapshot.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::stats::Histogram;

use crate::cache::{CacheStats, LogicalCache};
use crate::resources::{ByteBudget, StorageResources};
use crate::StorageError;

/// `(tenant, owner kind, label — `None` for every label —, pid)`.
pub type PlannerStatsKey = (TenantId, OwnerKind, Option<String>, u64);

/// One cached distribution (`None`: the property holds no range-comparable
/// values) and the commit index it was computed at.
#[derive(Clone, Debug)]
pub struct PlannerStatsEntry {
    /// The distribution, shared with every planner that read it.
    pub histogram: Option<Arc<Histogram>>,
    /// Snapshot commit index the walk read.
    pub computed_at_ci: u64,
}

struct Slot {
    entry: PlannerStatsEntry,
    /// Nanoseconds since the cache's epoch at the last use.
    last_used: AtomicU64,
}

/// Monotonic clock, injectable for tests.
pub type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// The store's planner-statistics cache.
pub struct PlannerStats {
    cache: Mutex<Option<LogicalCache<PlannerStatsKey, Slot>>>,
    idle: Duration,
    max_age_commits: u64,
    clock: Clock,
    epoch: Instant,
}

impl PlannerStats {
    fn stamp(&self) -> u64 {
        u64::try_from(
            (self.clock)()
                .saturating_duration_since(self.epoch)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX)
    }

    /// Build from the store's resources. `parent` is the logical cache
    /// budget; without one the cache gets its own budget.
    ///
    /// # Errors
    /// Returns [`StorageError::InvalidConfiguration`] if the budget cannot be
    /// created.
    pub fn new(
        resources: &StorageResources,
        parent: Option<&ByteBudget>,
        clock: Clock,
    ) -> Result<Self, StorageError> {
        let invalid =
            |e: crate::resources::ResourceError| StorageError::InvalidConfiguration(e.to_string());
        let cache = if resources.planner_stats_bytes == 0 {
            None
        } else {
            let budget = match parent {
                Some(parent) => parent
                    .child(

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
                        "planner-stats",
                        resources.planner_stats_bytes.min(parent.snapshot().limit),
                    )
                    .map_err(invalid)?,
                None => ByteBudget::new("planner-stats", resources.planner_stats_bytes)
                    .map_err(invalid)?,
            };
            Some(LogicalCache::new(budget).map_err(invalid)?)
        };
        Ok(Self {
            cache: Mutex::new(cache),
            idle: Duration::from_secs(resources.planner_stats_idle_seconds),
            max_age_commits: resources.planner_stats_max_age_commits,
            epoch: clock(),
            clock,
        })
    }

    /// The cached entry for `key`, unless missing or older than
    /// `max_age_commits` relative to `ci`. A hit refreshes the idle clock.
    #[must_use]
    pub fn get(&self, key: &PlannerStatsKey, ci: u64) -> Option<PlannerStatsEntry> {
        let now = self.stamp();
        let mut guard = self.cache.lock().ok()?;
        let cache = guard.as_mut()?;
        let slot = cache.get(key)?;
        if ci.saturating_sub(slot.entry.computed_at_ci) > self.max_age_commits {
            return None;
        }
        slot.last_used.store(now, Ordering::Relaxed);
        Some(slot.entry.clone())
    }

    /// Cache `entry` for `key`, evicting least-recently-used entries if the
    /// budget needs room. Returns whether it is resident.
    pub fn insert(&self, key: PlannerStatsKey, entry: PlannerStatsEntry) -> bool {
        let bytes = std::mem::size_of::<Slot>()
            + std::mem::size_of::<PlannerStatsKey>()
            + key.2.as_ref().map_or(0, String::capacity)
            + entry.histogram.as_ref().map_or(0, |h| h.resident_bytes());
        let Ok(mut guard) = self.cache.lock() else {
            return false;
        };
        let Some(cache) = guard.as_mut() else {
            return false;
        };
        let slot = Slot {
            entry,
            last_used: AtomicU64::new(self.stamp()),
        };
        cache.insert(key, slot, bytes)
    }

    /// Drop every entry unused for longer than the idle TTL. Returns how
    /// many were dropped.
    pub fn evict_idle(&self) -> usize {
        let now = self.stamp();
        let idle = u64::try_from(self.idle.as_nanos()).unwrap_or(u64::MAX);
        let Ok(mut guard) = self.cache.lock() else {
            return 0;
        };
        guard.as_mut().map_or(0, |cache| {
            cache
                .retain(|_, slot| now.saturating_sub(slot.last_used.load(Ordering::Relaxed)) < idle)
        })
    }

    /// Occupancy and counters (zero when caching is disabled).
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        self.cache
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(LogicalCache::stats))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {

//! Transitional fallible, pinned reads. Persistent views own one native redb
//! transaction per physical shard (merged on read); the memory reference uses
//! logical MVCC without cloning a root. Legacy callers remain separate until
//! the LMPG/query API migration.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::{
    prefix_range, redb_backend::MergedViews, resources::physical_shard_for, StorageError, Store,
};
use rushdb_types::ids::TenantId;

/// Per-read byte ceilings enforced when copying values out of the store.
#[derive(Clone, Copy, Debug)]
pub struct ReadLimits {
    /// Maximum copied key plus value for a single result.
    pub max_item_bytes: usize,
    /// Maximum encoded bytes in one current MVCC history block.
    pub max_history_block_bytes: usize,
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            max_item_bytes: 8 * 1024 * 1024,
            max_history_block_bytes: 16 * 1024 * 1024,
        }
    }
}

impl ReadLimits {
    pub(crate) fn check_item(
        &self,
        key_bytes: usize,
        value_bytes: usize,
    ) -> Result<(), StorageError> {
        if key_bytes
            .checked_add(value_bytes)
            .is_none_or(|size| size > self.max_item_bytes)
        {
            return Err(StorageError::ResourceExhausted(
                "read item exceeds max_item_bytes".into(),
            ));
        }
        Ok(())
    }
}

/// Shared cancellation flag for one or more protected read views.
#[derive(Clone, Default, Debug)]
pub struct ReadCancellation(Arc<AtomicBool>);

impl ReadCancellation {
    /// Request cancellation of reads sharing this flag.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub(crate) fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn check(&self) -> Result<(), StorageError> {
        if self.0.load(Ordering::Acquire) {
            Err(StorageError::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// One commit index's retained read lease.
///
/// `generation` identifies the set of live views sharing this entry. It changes
/// whenever the entry is re-created after being emptied or expired, so a stale
/// `ReadView` that drops *after* GC expired its lease cannot decrement a newer,
/// unrelated pin that happens to sit at the same commit index.
#[derive(Debug, Clone, Copy)]
pub struct ReadPin {
    /// Number of live views currently pinned at this commit index.
    pub count: usize,
    /// Wall-clock millis of the most recent pin admission (lease age base).
    pub lease_ms: u64,
    /// Identity of this pin generation (see type docs).
    pub generation: u64,
}

static PIN_GENERATION: AtomicU64 = AtomicU64::new(1);

fn next_pin_generation() -> u64 {
    PIN_GENERATION.fetch_add(1, Ordering::Relaxed)
}

/// Owns all resources needed to read; it can outlive the Store and be shared
/// with asynchronous providers without self-referential borrowed lifetimes.
pub struct ReadView {
    health: Arc<crate::health::StoreHealth>,
    pins: Arc<Mutex<BTreeMap<u64, ReadPin>>>,
    cancellations: Arc<Mutex<BTreeMap<u64, Vec<ReadCancellation>>>>,
    ci: u64,
    // The store's apply-time deriver, so an overlay over this view can derive
    // its staged writes. Derived rows of committed state are ordinary
    // rows of the snapshot.
    deriver: Option<Arc<dyn crate::Deriver>>,
    // The store's planner-statistics cache; estimates only.
    planner_stats: Arc<crate::PlannerStats>,
    // Generation of the `ReadPin` entry this view admitted into. Guards Drop
    // against decrementing a newer pin at the same CI after lease expiry.
    pin_generation: u64,
    // Whether the logical cap equals the backend view's current CI. This
    // remains true across later writes because the backend pins its pages.
    native_current: bool,
    // One read view per physical shard, merged on read. A key routes to exactly
    // one shard, so no in-memory mirror is needed. `None` on a bounded child,
    // which delegates to its parent.
    views: Option<MergedViews>,
    // Must drop after `views`: their cache charge covers the read views too.
    backends: Vec<Arc<dyn crate::StorageBackend>>,
    limits: ReadLimits,
    cancellation: ReadCancellation,
    // A bounded child keeps the parent's pinned storage transaction alive and
    // only narrows copying limits. This avoids reopening a later snapshot.
    parent: Option<Arc<ReadView>>,
    materialization_limit: usize,
}

impl Store {
    /// Open a pinned read view of the latest committed state.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the publisher has failed, the native
    /// backend read fails, or the default read limits are invalid.
    pub fn read_view(&self) -> Result<ReadView, StorageError> {
        self.read_with_limits(None, ReadLimits::default())
    }

    /// Open a pinned read view restricted to the physical shard that owns
    /// `tenant`. This avoids opening every shard for tenant-scoped engine
    /// requests while preserving the same snapshot and lease semantics.
    ///
    /// # Errors
    /// Returns the same errors as [`Store::read_view`].
    pub fn read_view_for_tenant(&self, tenant: &TenantId) -> Result<ReadView, StorageError> {
        self.read_with_limits_for_shard(None, ReadLimits::default(), Some(tenant))
    }

    /// Open a pinned read view of the state at `commit_index`.
    ///
    /// **Internal retention/backup mechanism, not a client contract.** There is
    /// no protocol surface for arbitrary snapshots (see); callers are
    /// limited to storage internals, offline PITR export, and the transaction
    /// coordinator's current-snapshot pin. It is `pub` only because those
    /// callers live in other crates.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the publisher has failed, the native
    /// backend read fails, or the requested snapshot is in the future or
    /// expired.
    #[doc(hidden)]
    pub fn read_at(&self, commit_index: u64) -> Result<ReadView, StorageError> {
        self.read_with_limits(Some(commit_index), ReadLimits::default())
    }

    /// Open a pinned read view with explicit `limits`, optionally at a
    /// requested commit index.
    ///
    /// # Errors
    /// Returns [`StorageError::InvalidRead`] for zero limits,
    /// [`StorageError::FutureSnapshot`] when `requested` is newer than the
    /// visible commit, [`StorageError::SnapshotExpired`] when it predates the
    /// GC floor, or a backend error if the native view cannot be opened.
    ///
    /// Lock poisoning fences the store until reopen.
    pub fn read_with_limits(
        &self,
        requested: Option<u64>,
        limits: ReadLimits,
    ) -> Result<ReadView, StorageError> {
        self.read_with_limits_for_shard(requested, limits, None)
    }

    fn read_with_limits_for_shard(
        &self,
        requested: Option<u64>,
        limits: ReadLimits,
        tenant: Option<&TenantId>,
    ) -> Result<ReadView, StorageError> {
        if limits.max_item_bytes == 0 || limits.max_history_block_bytes == 0 {
            return Err(StorageError::InvalidRead(
                "read byte limits must be positive".into(),
            ));
        }
        // Publication does not need the writer's publish lock: a native read
        // transaction pins its own physical snapshot, and `committed_version`
        // is published only after the durable commit. Admission is serialized
        // with GC floor advancement on `read_pins` alone (see
        // `retire_before_locked`), so opening a view no longer contends with an
        // in-flight fsync.
        self.check_health()?;
        self.check_publisher()?;
        // Native direct reads open one transaction per physical shard. A single
        // shard uses the optimized current-read path; multiple shards merge
        // their native views per key (the global LSN is unchanged), so reads
        // stay memory-bounded with no in-memory mirror.
        let backends: Vec<Arc<dyn crate::StorageBackend>> = match tenant {
            Some(tenant) if self.resource_config().shard_count > 1 => self
                .backend(physical_shard_for(
                    Some(tenant),
                    self.resource_config().shard_count,
                ))
                .into_iter()
                .collect(),
            _ if self.resource_config().shard_count == 1 => {
                self.primary_backend().into_iter().collect()
            }
            _ => self.all_backends(),
        };
        let has_backends = !backends.is_empty();
        let mut views: Vec<Box<dyn crate::backend::StorageReadView>> =
            Vec::with_capacity(backends.len());
        for backend in &backends {
            views.push(backend.read_view()?);
        }
        let merged = MergedViews::new(views);
        let (visible, floor) = match (has_backends, tenant) {
            // A tenant-routed native transaction may lag the global watermark
            // because another tenant committed on a different shard. The
            // routed shard is still complete for this tenant, so retain the
            // store-wide snapshot identity and retention floor.
            (true, Some(_)) | (false, _) => (self.committed_version(), self.gc_watermark()),
            (true, None) => (merged.visible_ci()?, merged.history_floor()?),
        };
        let ci = requested.unwrap_or(visible);
        if ci > visible {
            return Err(StorageError::FutureSnapshot {
                requested: ci,
                visible,
            });
        }
        // Serialize the floor check and pin insert with GC floor advancement.
        // GC stores the new watermark before it reclaims, so any pin inserted
        // here after the floor moved is rejected; any pin inserted before is
        // included in GC's clamp.
        let mut pins = self.health.guard(self.read_pins.lock())?;
        self.check_publisher()?;
        let floor = floor.max(self.gc_watermark());
        if ci < floor {
            return Err(StorageError::SnapshotExpired {
                requested: ci,
                oldest: floor,
            });
        }
        // The pin carries a lease timestamp so GC can expire abandoned readers
        // by age instead of blocking history forever. Refresh it on every
        // admission: a long-lived entry with churning readers is still active
        // and must not be expired out from under them.
        let lease_ms = crate::wal::now_ms();
        let cancellation = ReadCancellation::default();
        self.health
            .guard(self.read_cancellations.lock())?
            .entry(ci)
            .or_default()
            .push(cancellation.clone());
        let pin_generation = {
            let entry = pins.entry(ci).or_insert_with(|| ReadPin {
                count: 0,
                lease_ms,
                generation: next_pin_generation(),
            });
            entry.count += 1;
            entry.lease_ms = lease_ms;
            entry.generation
        };
        let deriver = self.deriver();
        Ok(ReadView {
            health: Arc::clone(&self.health),
            pins: self.read_pins.clone(),
            cancellations: self.read_cancellations.clone(),
            ci,
            deriver,
            planner_stats: Arc::clone(&self.planner_stats),
            pin_generation,
            native_current: ci == visible,
            views: Some(merged),
            backends,
            limits,
            cancellation,
            parent: None,
            materialization_limit: 64 * 1024 * 1024,
        })
    }
}

impl Drop for ReadView {
    fn drop(&mut self) {
        // Bounded children share the root pin through their parent Arc.
        if self.parent.is_some() {
            return;
        }
        // Release the native pages before admitting physical compaction.
        self.views.take();
        self.backends.clear();
        let mut pins = self.health.cleanup(self.pins.lock());
        if let Some(entry) = pins.get_mut(&self.ci) {
            // Only release a pin this view actually admitted. If GC expired our
            // lease and a new generation was admitted at the same CI, touching
            // it here would drop a live reader's retention.
            if entry.generation == self.pin_generation {
                entry.count = entry.count.saturating_sub(1);
                if entry.count == 0 {
                    pins.remove(&self.ci);
                }
            }
        }
        let mut cancellations = self.health.cleanup(self.cancellations.lock());
        if let Some(entries) = cancellations.get_mut(&self.ci) {
            entries.retain(|entry| !entry.same_as(&self.cancellation));
            if entries.is_empty() {
                cancellations.remove(&self.ci);
            }
        }
    }
}

impl ReadView {
    /// The store's planner-statistics cache. Estimates only; exact
    /// answers never come from it.
    #[must_use]
    pub fn planner_stats_cache(&self) -> &crate::PlannerStats {
        &self.planner_stats
    }

    fn storage_view(&self) -> &ReadView {
        self.parent.as_deref().map_or(self, ReadView::storage_view)
    }

    #[cfg(test)]
    pub(crate) fn native_backend_count(&self) -> usize {
        self.backends.len()
    }

    /// Process-local Store identity, independent of tenant and commit index.
    /// Prevents accidentally pairing a view with another engine's catalogs.
    pub fn belongs_to(&self, store: &Store) -> bool {
        Arc::ptr_eq(&self.pins, &store.read_pins)
    }

    /// Commit index this view reads at.
    #[must_use]
    pub fn commit_index(&self) -> u64 {
        self.ci
    }

    /// The store's apply-time deriver, if one is installed.
    #[must_use]
    pub fn deriver(&self) -> Option<&Arc<dyn crate::Deriver>> {
        self.deriver.as_ref()
    }

    /// A cloneable handle to this view's cancellation flag.
    #[must_use]
    pub fn cancellation(&self) -> ReadCancellation {
        self.cancellation.clone()
    }

    /// Create a child view over this exact pinned snapshot with narrower
    /// per-item and materialized-prefix limits. The child has its own limits,
    /// while cancellation remains shared with the parent.
    #[must_use]
    pub fn with_materialization_limit(self: &Arc<Self>, bytes: usize) -> Arc<ReadView> {
        Arc::new(ReadView {
            health: Arc::clone(&self.health),
            pins: self.pins.clone(),
            cancellations: self.cancellations.clone(),
            ci: self.ci,
            deriver: self.deriver.clone(),
            planner_stats: Arc::clone(&self.planner_stats),
            pin_generation: self.pin_generation,
            native_current: self.native_current,
            views: None,
            backends: Vec::new(),
            limits: ReadLimits {
                max_item_bytes: self.limits.max_item_bytes.min(bytes),
                max_history_block_bytes: self.limits.max_history_block_bytes,
            },
            cancellation: self.cancellation.clone(),
            parent: Some(self.clone()),
            materialization_limit: self.materialization_limit.min(bytes),
        })
    }

    /// Check cancellation between non-storage operations (for example,
    /// serializing output) without performing an otherwise unnecessary read.
    ///
    /// # Errors
    /// Returns [`StorageError::Cancelled`] if the view has been cancelled.
    pub fn check_cancelled(&self) -> Result<(), StorageError> {
        self.health.check()?;
        self.cancellation.check()
    }

    /// Read a single key at this view's snapshot.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the view is cancelled, the item exceeds the
    /// read limit, or the backend read fails.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.read_value(key, ScanProjection::KeyValue)
    }

    /// Transitional materializing helper for consumers migrating to cursors.
    /// The local ceiling is not a substitute for shared query admission.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the view is cancelled, the item exceeds the
    /// read limit, or the materialized result exceeds the 64 MiB ceiling.
    ///
    /// # Panics
    /// Panics if a key/value projection batch unexpectedly yields a row
    /// without a value.
    pub fn range_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.materialize_scan(ScanBounds::prefix(prefix))
    }

    /// Materialize every key/value pair with `start <= key < end`, in key
    /// order. An empty `end` means unbounded above. Shares the prefix path's
    /// aggregate materialization ceiling.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the view is cancelled, the item exceeds the
    /// read limit, or the materialized result exceeds the 64 MiB ceiling.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let bounds = ScanBounds {
            start: Bound::Included(start.to_vec()),
            end: if end.is_empty() {
                Bound::Unbounded
            } else {
                Bound::Excluded(end.to_vec())
            },
        };

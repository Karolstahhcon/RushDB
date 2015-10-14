//! Tenant residency on demand: one registry owns the per-tenant
//! in-memory handles (vector engine, transaction coordinator). Idle tenants
//! cost about nothing:
//!
//! - an entry is created when a request first needs it;
//! - an entry nobody holds, with no open transaction, idle longer than
//!   `tenant-idle-seconds`, is evicted (and rebuilt on the next request —
//!   all of its state is durable);
//! - over `tenant-resident-bytes`, least-recently-used idle entries go
//!   first;
//! - when every resident entry is active, admitting a new tenant fails with
//!   a retryable `tenant_capacity` error (HTTP 503): a load signal, not a
//!   fixed tenant count.
//!
//! Text indexes, planner statistics and the index catalog are store-wide
//! and are bounded there, not here.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rushdb_types::ids::TenantId;

/// Fixed accounting charge per resident tenant (registry entry, coordinator
/// and engine shells), on top of what the handles report.
pub(crate) const ENTRY_OVERHEAD_BYTES: usize = 16 * 1024;

/// Monotonic clock, injectable for tests.
pub(crate) type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

#[derive(Default)]
struct Resident {
    vector: Option<Arc<rushdb_vector::VectorEngine>>,
    tx: Option<Arc<rushdb_tx::TxCoordinator>>,
    last_used: Option<Instant>,
}

impl Resident {
    fn bytes(&self) -> usize {
        ENTRY_OVERHEAD_BYTES
            + self
                .vector
                .as_ref()
                .map_or(0, |engine| engine.resident_bytes())
    }

    /// No request holds a handle and no transaction is open.
    fn idle(&self) -> bool {
        self.vector
            .as_ref()
            .is_none_or(|engine| Arc::strong_count(engine) == 1)
            && self.tx.as_ref().is_none_or(|coordinator| {
                Arc::strong_count(coordinator) == 1 && coordinator.active_count() == 0
            })
    }
}

/// Error when every resident tenant is active and the budget is spent.
pub(crate) fn capacity_error(budget: usize) -> String {
    format!(
        "tenant_capacity: every resident tenant is active and the {budget}-byte residency budget is spent; retry later"
    )
}

/// The node's tenant residency registry.
pub(crate) struct TenantResidency {
    entries: Mutex<HashMap<TenantId, Resident>>,
    idle: Duration,
    budget: usize,
    clock: Clock,
}

impl TenantResidency {
    pub(crate) fn new(idle: Duration, budget: usize, clock: Clock) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            idle,
            budget,
            clock,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<TenantId, Resident>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Bytes charged by every resident entry.
    pub(crate) fn resident_bytes(&self) -> usize {
        self.lock().values().map(Resident::bytes).sum()
    }

    /// Resident tenants, most recently used first.
    /// Every resident vector engine (for background segment maintenance).
    pub(crate) fn resident_vector_engines(&self) -> Vec<Arc<rushdb_vector::VectorEngine>> {
        self.lock()
            .values()
            .filter_map(|resident| resident.vector.clone())
            .collect()
    }

    pub(crate) fn resident_tenants(&self) -> Vec<TenantId> {
        let entries = self.lock();
        let mut tenants: Vec<(TenantId, Option<Instant>)> = entries
            .iter()
            .map(|(tenant, resident)| (*tenant, resident.last_used))
            .collect();
        tenants.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        tenants.into_iter().map(|(tenant, _)| tenant).collect()
    }

    /// Make room for `tenant`'s entry: evict idle entries least recently
    /// used first until the budget holds one more entry, or fail when only
    /// active entries remain.
    fn admit(
        &self,
        entries: &mut HashMap<TenantId, Resident>,
        tenant: TenantId,
    ) -> Result<(), String> {
        if entries.contains_key(&tenant) {
            return Ok(());
        }
        loop {
            let used: usize = entries.values().map(Resident::bytes).sum();
            if used.saturating_add(ENTRY_OVERHEAD_BYTES) <= self.budget {
                return Ok(());
            }
            let victim = entries
                .iter()
                .filter(|(_, resident)| resident.idle())
                .min_by_key(|(_, resident)| resident.last_used)
                .map(|(tenant, _)| *tenant);
            match victim {
                Some(victim) => {
                    entries.remove(&victim);
                }
                None => return Err(capacity_error(self.budget)),
            }
        }
    }

    /// Record that `tenant` is being served. A handle-less entry is always
    /// idle, so a touch never refuses admission; over budget it evicts idle
    /// entries least recently used first, and is itself skipped when only
    /// active entries remain.
    pub(crate) fn touch(&self, tenant: TenantId) {
        let now = (self.clock)();
        let mut entries = self.lock();
        if !entries.contains_key(&tenant) && self.admit(&mut entries, tenant).is_err() {
            return;
        }
        entries.entry(tenant).or_default().last_used = Some(now);
    }

    /// The tenant's vector engine, created by `make` on first use.
    pub(crate) fn vector(
        &self,
        tenant: TenantId,
        make: impl FnOnce() -> rushdb_vector::VectorEngine,
    ) -> Result<Arc<rushdb_vector::VectorEngine>, String> {
        let now = (self.clock)();
        let mut entries = self.lock();
        self.admit(&mut entries, tenant)?;
        let resident = entries.entry(tenant).or_default();
        resident.last_used = Some(now);
        Ok(Arc::clone(
            resident.vector.get_or_insert_with(|| Arc::new(make())),
        ))
    }

    /// The tenant's transaction coordinator, created by `make` on first use.
    pub(crate) fn tx(
        &self,
        tenant: TenantId,
        make: impl FnOnce() -> Result<rushdb_tx::TxCoordinator, String>,
    ) -> Result<Arc<rushdb_tx::TxCoordinator>, String> {
        let now = (self.clock)();
        let mut entries = self.lock();
        self.admit(&mut entries, tenant)?;
        let resident = entries.entry(tenant).or_default();
        resident.last_used = Some(now);
        if let Some(coordinator) = &resident.tx {
            return Ok(Arc::clone(coordinator));
        }
        let coordinator = Arc::new(make()?);
        resident.tx = Some(Arc::clone(&coordinator));
        Ok(coordinator)
    }

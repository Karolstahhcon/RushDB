//! Bounded in-memory window of per-commit write sets used for optimistic
//! conflict validation.
//!
//! A reader validated at commit time must know which keys every commit
//! published after its snapshot wrote. Those readers are live statements and
//! transactions: they are bounded by `max_txn_ttl` and never survive a
//! restart. The write sets therefore live in memory only. They are not
//! WAL ops and not redb rows. Persisting them as JSON number arrays cost
//! ~13.7 KB per record on disk and roughly half of every WAL record
//! (C4, cloud-capacity F1).
//!
//! The window keeps the newest commits under a byte cap and evicts oldest
//! first. A validation that needs an evicted commit fails with the retryable
//! [`crate::StorageError::ConflictHistoryUnavailable`], never with a missed
//! conflict.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::Op;

/// Default byte cap for the retained write sets.
const DEFAULT_CONFLICT_WINDOW_BYTES: usize = 64 * 1024 * 1024;

/// Accounting overhead charged per retained key, on top of its length.
const PER_KEY_OVERHEAD: usize = 24;

/// Write sets of recently published commits, oldest first.
pub(crate) struct ConflictWindow {
    entries: VecDeque<(u64, Arc<[Vec<u8>]>, usize)>,
    bytes: usize,
    cap: usize,
    /// The window's bytes charged to the store's managed budget. When the
    /// budget cannot admit more, the oldest write sets go first.
    charge: Option<crate::resources::ByteReservation>,
}

impl std::fmt::Debug for ConflictWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConflictWindow")
            .field("entries", &self.entries.len())
            .field("bytes", &self.bytes)
            .field("cap", &self.cap)
            .finish_non_exhaustive()
    }
}

impl ConflictWindow {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            cap,
            charge: None,
        }
    }

    /// A window of the default cap whose bytes are charged to `budget`.
    pub(crate) fn charged_to(budget: &crate::resources::ByteBudget) -> Self {
        Self::charged(DEFAULT_CONFLICT_WINDOW_BYTES, budget)
    }

    pub(crate) fn charged(cap: usize, budget: &crate::resources::ByteBudget) -> Self {
        Self {
            charge: budget.try_reserve(0).ok(),
            ..Self::new(cap)
        }
    }

    /// Charge the window's current bytes; `false` when the budget refuses.
    fn charge_bytes(&mut self) -> bool {
        let bytes = self.bytes;
        self.charge
            .as_mut()
            .is_none_or(|charge| charge.resize(bytes).is_ok())
    }

    fn evict_oldest(&mut self) {
        if let Some((_, _, evicted)) = self.entries.pop_front() {
            self.bytes -= evicted;
        }
    }

    /// Record the application-visible write set of commit `ci`.
    ///
    /// Commits are recorded in publication (ascending index) order.
    /// Coordination rows such as the outcome, journal and watermark keys are
    /// excluded, because every commit rewrites them and they are never read
    /// dependencies.
    pub(crate) fn record(&mut self, ci: u64, ops: &[Op]) {
        let keys: Arc<[Vec<u8>]> = ops
            .iter()
            .map(op_key)
            .filter(|key| !crate::is_coordination_write(key))
            .cloned()
            .collect();
        let size = keys.iter().map(|key| key.len() + PER_KEY_OVERHEAD).sum();
        self.entries.push_back((ci, keys, size));
        self.bytes += size;
        // Keep the newest commit even when it alone exceeds the cap (the
        // next validation most likely needs exactly that one), but never
        // hold bytes the managed budget refused: an evicted write set only
        // makes a validation that needs it fail retryably.
        while self.bytes > self.cap && self.entries.len() > 1 {
            self.evict_oldest();

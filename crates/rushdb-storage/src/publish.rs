//! Ordered group publication: the publisher drains contiguous pending
//! commits into one WAL append + fsync and one backend transaction (base plus
//! derived rows, DC12), then records the conflict window in the same critical
//! section.

use std::collections::BTreeMap;

use super::{
    derive, wal, CommitPhase, DeriveRead, Op, Ordering, PendingCommit, PreparedCommand,
    StorageError, Store,
};

impl Store {
    /// V2 WAL batching: hold the publish lock briefly so concurrent commits
    /// can join this group and share one fsync. A no-op when
    /// `max_group_delay_ms` is 0. Returns early once the count cap is reached.
    pub(super) fn linger_for_group(&self) {
        let delay_ms = self.resource_config.max_group_delay_ms;
        if delay_ms == 0 {
            return;
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(delay_ms);
        let mut pending = self.health.cleanup(self.pending.lock());
        loop {
            let cap = self.resource_config.max_group_count;
            if cap > 0 && pending.len() >= cap {
                return;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return;
            }
            let (guard, _) = self
                .health
                .cleanup(self.pending_cv.wait_timeout(pending, remaining));
            pending = guard;
        }
    }

    /// Group-commit publisher: drain the contiguous prefix from the published
    /// watermark, persist ALL drained op-sets in ONE redb write transaction,

//! Apply-time derivation.
//!
//! Derived rows (presence, value dictionary, counters, declared-index
//! entries) are computed by one deterministic [`Deriver`] from a commit's
//! *base* ops plus the state before that commit, and written in the same
//! backend batch at the same commit index. They are never logged in the WAL
//! and never enter a transaction's write set, so they cannot cause write
//! conflicts. Publication, command replay, and WAL replay (crash recovery,
//! PITR, replicas) all call [`Store::derive_ops`], so every node derives the
//! same rows from the same base log.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use crate::backend::StorageReadView;
use crate::read_view::{ReadLimits, ScanProjection};
use crate::{Op, StorageError, Store};

/// Read access to the state *before* a commit, for a [`Deriver`].
pub trait DeriveRead {
    /// Current value of `key`, or `None` when absent.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the underlying read fails.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;

    /// Every live `(key, value)` whose key starts with `prefix`, in key order.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the underlying read fails.
    fn range_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>;

    /// Every live `(key, value)` with `start <= key < end`, in key order. An
    /// empty `end` is unbounded above.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the underlying read fails.
    fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>;
}

/// Rows of `map` in `[start, end)` (empty `end` = unbounded).
fn map_range<'a, V: 'a>(
    map: &'a BTreeMap<Vec<u8>, V>,
    start: &[u8],
    end: &[u8],
) -> impl Iterator<Item = (&'a Vec<u8>, &'a V)> {
    let upper = if end.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Excluded(end.to_vec())
    };
    map.range::<Vec<u8>, _>((Bound::Included(start.to_vec()), upper))
}

/// Deterministic derivation of derived rows from a commit's base ops.
///
/// Implementations must be pure functions of `(ci, base_ops, before)`: no
/// clock, randomness, or global state, because replay and replicas call the
/// same function and must reach byte-identical state.
pub trait Deriver: Send + Sync {
    /// Materialized derived ops (final values, not deltas) for one commit.
    ///
    /// # Errors
    /// Returns [`StorageError`] when the base ops or the prior state are
    /// inconsistent; the commit (or replay) then fails.
    fn derive(
        &self,
        ci: u64,
        base_ops: &[Op],
        before: &dyn DeriveRead,
    ) -> Result<Vec<Op>, StorageError>;

    /// Whether `key` belongs to a derived family (so an overlay must derive
    /// its staged writes before answering a read of it).
    fn is_derived_key(&self, key: &[u8]) -> bool;

    /// Whether a scan of `prefix` can reach derived keys.
    fn prefix_reaches_derived(&self, prefix: &[u8]) -> bool;
}

impl DeriveRead for crate::ReadView {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        crate::ReadView::get(self, key)
    }

    fn range_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        crate::ReadView::range_prefix(self, prefix)
    }

    fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        crate::ReadView::range(self, start, end)
    }
}

/// A read base plus a buffer of writes not yet visible in it (`None` =
/// tombstone): the state before a commit inside a publish group.
pub(crate) struct Layered<'a> {
    pub(crate) base: &'a dyn DeriveRead,
    pub(crate) buffer: &'a BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl DeriveRead for Layered<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        match self.buffer.get(key) {
            Some(value) => Ok(value.clone()),
            None => self.base.get(key),
        }
    }

    fn range_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> =
            self.base.range_prefix(prefix)?.into_iter().collect();
        let upper = crate::prefix_range(prefix).1;
        let end = if upper.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(upper)
        };
        for (key, value) in self
            .buffer
            .range::<Vec<u8>, _>((Bound::Included(prefix.to_vec()), end))
        {
            match value {
                Some(value) => {
                    merged.insert(key.clone(), value.clone());
                }
                None => {
                    merged.remove(key);
                }
            }
        }
        Ok(merged.into_iter().collect())
    }

    fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> =
            self.base.range(start, end)?.into_iter().collect();
        for (key, value) in map_range(self.buffer, start, end) {
            match value {
                Some(value) => {
                    merged.insert(key.clone(), value.clone());
                }
                None => {
                    merged.remove(key);
                }
            }
        }

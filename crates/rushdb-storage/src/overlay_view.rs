//! Owned, read-buffer-first view over a pinned snapshot.
//!
//! An [`OverlayView`] layers an uncommitted write buffer (`None` = tombstone)
//! on top of a [`ReadView`]. It is the single primitive behind transaction
//! read-your-writes (a statement sees earlier statements' staged writes) and,
//! later, the sequencer's speculative expansion layer. Reads consult the buffer
//! first, then the pinned base snapshot, so the overlay never blocks a reader
//! and never mutates committed state.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;

use crate::{Op, ReadCancellation, ReadView, ScanBounds, ScanProjection, StorageError};

/// A pinned base snapshot plus an uncommitted key overlay.
///
/// `Clone` is cheap for the base (`Arc`) and copies the overlay map; callers
/// that only read should share one instance behind an `Arc`.
#[derive(Clone)]
pub struct OverlayView {
    base: Arc<ReadView>,
    buffer: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    // Derived rows implied by the staged writes, computed lazily on the first
    // read of a derived key and reset by every write, so a transaction's own
    // writes are visible to derived reads (T-5b for aggregates, DC12).
    derived: std::sync::OnceLock<Result<Arc<BTreeMap<Vec<u8>, Option<Vec<u8>>>>, StorageError>>,
}

impl std::fmt::Debug for OverlayView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverlayView")
            .field("commit_index", &self.base.commit_index())
            .field("overlay_keys", &self.buffer.len())
            .finish_non_exhaustive()
    }
}

impl OverlayView {
    /// Wrap a pinned snapshot with an empty overlay.
    #[must_use]
    pub fn new(base: Arc<ReadView>) -> Self {
        Self {
            base,
            buffer: BTreeMap::new(),
            derived: std::sync::OnceLock::new(),
        }
    }

    /// Wrap a pinned snapshot with a pre-existing overlay buffer.
    #[must_use]
    pub fn with_buffer(base: Arc<ReadView>, buffer: BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Self {
        Self {
            base,
            buffer,
            derived: std::sync::OnceLock::new(),
        }
    }

    /// Derived rows of the staged writes against the base (lazy, cached until
    /// the next write).
    fn derived_rows(&self) -> Result<Arc<BTreeMap<Vec<u8>, Option<Vec<u8>>>>, StorageError> {
        self.derived
            .get_or_init(|| {
                let mut rows = BTreeMap::new();
                if let Some(deriver) = self.base.deriver() {
                    if !self.buffer.is_empty() {
                        let ops: Vec<Op> = self
                            .buffer
                            .iter()
                            .map(|(key, value)| match value {
                                Some(value) => Op::Put(key.clone(), value.clone()),
                                None => Op::Del(key.clone()),
                            })
                            .collect();
                        crate::derive::fold(&mut rows, &deriver.derive(0, &ops, &*self.base)?);
                    }
                }
                Ok(Arc::new(rows))
            })
            .clone()
    }

    fn reaches_derived(&self, prefix: &[u8]) -> bool {
        !self.buffer.is_empty()
            && self
                .base
                .deriver()
                .is_some_and(|deriver| deriver.prefix_reaches_derived(prefix))
    }

    /// The pinned base snapshot underneath this overlay.
    #[must_use]
    pub fn base(&self) -> &Arc<ReadView> {
        &self.base
    }

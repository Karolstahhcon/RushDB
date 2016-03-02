//! Narrow storage-substrate seam (research D6, plan W3).
//!
//! `StorageBackend` captures the durability/write surface the [`Store`] needs
//! from its physical substrate. The synchronous redb implementation
//! (`RedbBackend`) satisfies it today; an async backend (`io_uring` / direct IO)
//! or a future `RushDB` `PageStore` can replace it without touching LMPG, query,
//! search, or the replication/transaction layers, because those layers depend
//! only on this trait and on the concrete read-view type.
//!
//! Greenfield note: the read view (`RedbReadView`) remains concrete in V1 — it
//! is the synchronous backend's owned snapshot. An async backend brings its own
//! view type behind this same seam; the point of the trait is that the write /
//! durability contract is already isolated.

use crate::read_view::{ReadLimits, ScanProjection};
use crate::resources::ByteReservation;
use crate::{Op, StorageError, StorageStatistics, VersionEntry};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

/// Object-safe read snapshot of one physical backend.
///
/// Both the redb view (`RedbReadView`) and the in-memory view
/// (`MemoryReadView`) implement it, so [`crate::ReadView`] has one read path
/// regardless of substrate. Multi-shard stores merge the per-shard
/// views in `MergedViews`.
pub(crate) trait StorageReadView: Send + Sync {
    /// Point read at `ci`; `current` selects the latest-commit fast path.
    fn read_value(
        &self,
        key: &[u8],
        ci: u64,
        current: bool,
        limits: ReadLimits,
        projection: ScanProjection,
    ) -> Result<Option<Vec<u8>>, StorageError>;

    /// Smallest key in `(lower, upper)`; `include_history` widens to retained
    /// versions for a non-current snapshot.
    fn next_key(
        &self,
        lower: &Bound<Vec<u8>>,
        upper: &Bound<Vec<u8>>,

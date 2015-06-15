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
        limits: ReadLimits,
        include_history: bool,
    ) -> Result<Option<Vec<u8>>, StorageError>;

    /// Ordered current-value range with the requested projection.
    fn current_range<'a>(
        &'a self,
        start: &[u8],
        end: &[u8],
        limits: ReadLimits,
        projection: ScanProjection,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>> + 'a>, StorageError>;

    /// `ScanCursor`'s v2 native-iterator fast path (B7 follow-up): a genuine
    /// backend range cursor over `(lower, upper)` matching `ScanCursor`'s own
    /// `Bound` representation exactly, unlike [`Self::current_range`]'s
    /// simplified included-start/excluded-end-or-unbounded convention. A
    /// real cursor walks consecutively (one seek for the whole scan, not one
    /// per row); callers must already know this view's `current` fast path
    /// is valid — same gate [`Self::read_value`]'s `current` flag needs,
    /// since this reads the live current-value table.
    ///
    /// `Ok(None)` means this backend has no such cursor; the caller falls
    /// back to [`Self::next_key`] + [`Self::read_value`] per row. The
    /// default is `Ok(None)` so adding this is additive — a backend opts in
    /// explicitly.
    fn native_range<'a>(
        &'a self,
        _lower: &Bound<Vec<u8>>,
        _upper: &Bound<Vec<u8>>,
        _limits: ReadLimits,
        _projection: ScanProjection,
    ) -> Result<
        Option<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>> + 'a>>,
        StorageError,
    > {
        Ok(None)
    }

    /// Durable LSN this view can serve.
    fn visible_ci(&self) -> Result<u64, StorageError>;

    /// Oldest commit for which this view still retains history.
    fn history_floor(&self) -> Result<u64, StorageError>;


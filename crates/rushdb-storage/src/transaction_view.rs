//! Buffered transaction reads and dependency tracking over one storage view.

use super::{
    prefix_range, redb_backend, Arc, BTreeMap, DependencySet, Included, Op, OverlayView,
    ReadLimits, ReadView, RefCell, ScanBounds, ScanCursor, ScanProjection, ScanRow, StorageError,
    Unbounded,
};

/// A transaction's base read source, checked after its staged `buffer`.
/// Exactly one variant applies per transaction, chosen at construction —
/// there is no longer a "live data" field that sits unused (defaulted to a
/// static empty sentinel) whenever a snapshot/overlay/native base applies
/// instead.
pub(super) enum TxnBase<'a> {
    /// Committed-plus-pending overlay supplied by the sequencer: reads
    /// resolve through it (its own pending buffer over a committed
    /// snapshot) instead of any other base, since it is already
    /// authoritative.
    Overlay(Arc<OverlayView>),
    /// Snapshot pinned at BEGIN for a cross-request transaction: all base
    /// reads resolve at that commit index instead of current state, so the
    /// transaction sees a stable base plus its own overlay. Autocommit and
    /// recovery use a different variant and read current committed state
    /// under the writer admission lock.
    Snapshot(Arc<ReadView>),
    /// Native-backend merged view over current committed state.
    Native(redb_backend::MergedViews),
    /// The live committed-data map itself, borrowed for the transaction's
    /// duration (no native backend installed).
    Live(&'a BTreeMap<Vec<u8>, Vec<u8>>),
}

impl<'a> TxnBase<'a> {
    /// Choose a transaction's base in priority order: an overlay or pinned
    /// snapshot (callers pass at most one of the two — a transaction never
    /// carries both), else a native merged view, else the live
    /// committed-data map. `live` must be `Some` whenever none of the other
    /// three apply — callers only acquire the read-lock guard it derefs
    /// from when a cheaper base doesn't already apply.
    ///
    /// # Panics
    /// Panics if `live` is `None` while `overlay`, `snapshot`, and `native`
    /// are all also `None` — every call site arranges for one of the four
    /// to be available.
    pub(super) fn resolve(
        overlay: Option<Arc<OverlayView>>,
        snapshot: Option<Arc<ReadView>>,
        native: Option<redb_backend::MergedViews>,
        live: Option<&'a BTreeMap<Vec<u8>, Vec<u8>>>,
    ) -> Self {
        if let Some(overlay) = overlay {
            return TxnBase::Overlay(overlay);
        }
        if let Some(snapshot) = snapshot {
            return TxnBase::Snapshot(snapshot);
        }
        if let Some(native) = native {
            return TxnBase::Native(native);
        }
        TxnBase::Live(live.expect(
            "a transaction without an overlay, snapshot, or native view always has the live guard",
        ))
    }
}

/// Buffered view handed to admission/apply: reads see committed data merged
/// with staged writes; writes stay buffered until the closure succeeds.
pub struct Txn<'a> {
    pub(super) base: TxnBase<'a>,
    pub(super) buffer: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Keys staged via [`Txn::purge`]: their drained op is [`Op::Purge`]
    /// (history-erasing) instead of [`Op::Del`] (history-retaining).
    pub(super) purge_keys: std::collections::BTreeSet<Vec<u8>>,
    /// Keys staged via [`Txn::set`]: their drained op is [`Op::Set`]
    /// (history-free overwrite) instead of [`Op::Put`] (history-retaining).
    pub(super) set_keys: std::collections::BTreeSet<Vec<u8>>,
    pub(super) deps: Option<&'a RefCell<DependencySet>>,
}

/// Materialize a bounded slice of a pinned snapshot's range. `max_rows == 0`
/// means unbounded. Used by transaction reads so their base view is the
/// transaction snapshot rather than current committed state.
fn snapshot_scan(
    snapshot: &ReadView,
    start: &[u8],
    end: &[u8],
    projection: ScanProjection,
    max_rows: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
    let bounds = ScanBounds {
        start: Included(start.to_vec()),
        end: if end.is_empty() {
            Unbounded
        } else {
            std::ops::Bound::Excluded(end.to_vec())
        },
    };
    let mut cursor = snapshot.scan(bounds, projection)?;
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    loop {
        let remaining = if max_rows == 0 {
            1024
        } else {
            max_rows.saturating_sub(rows.len()).clamp(1, 1024)
        };
        let batch = cursor.next_batch(remaining, 8 * 1024 * 1024)?;
        for row in batch.rows {
            rows.push((row.key, row.value.unwrap_or_default()));
        }
        if batch.done || (max_rows != 0 && rows.len() >= max_rows) {
            break;
        }
    }
    Ok(rows)
}

/// Lazy key-only iterator over a pinned snapshot's rows in `[start, end)`.
/// Unlike [`snapshot_scan`] it does not materialize, so a caller can stop after
/// a bounded number of live keys without walking (or buffering) a whole prefix.
struct SnapshotKeyIter<'view> {
    cursor: ScanCursor<'view>,
    pending: std::vec::IntoIter<ScanRow>,
    done: bool,
}

impl<'view> SnapshotKeyIter<'view> {
    fn new(snapshot: &'view ReadView, start: &[u8], end: &[u8]) -> Result<Self, StorageError> {
        let bounds = ScanBounds {
            start: Included(start.to_vec()),
            end: if end.is_empty() {
                Unbounded
            } else {
                std::ops::Bound::Excluded(end.to_vec())
            },
        };
        Ok(Self {
            cursor: snapshot.scan(bounds, ScanProjection::Keys)?,
            pending: Vec::new().into_iter(),
            done: false,
        })

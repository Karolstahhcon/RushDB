use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{
    key, u64be, P_DEFINITION, P_FAMILY, P_FAMILY_SIG, P_REL, P_REL_BY_ORD, P_REL_GROUP,
};
use rushdb_types::model::TypeState;

type KeyValueRows = Vec<(Vec<u8>, Vec<u8>)>;

/// Fallible storage boundary for admission and migrated consumers. Legacy
/// Fallible storage boundary for admission and reads. Txn deliberately does
/// not implement an infallible counterpart, so a disk error cannot become a
/// missing record.
pub trait TryRead {
    /// Returns the value stored at `k`, or `None` if absent.
    ///
    /// # Errors
    /// Returns the underlying storage error on read failure.
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError>;
    /// Returns every key/value pair whose key starts with `p`.
    ///
    /// # Errors
    /// Returns the underlying storage error on scan failure.
    fn range_prefix(&self, p: &[u8]) -> Result<KeyValueRows, rushdb_storage::StorageError>;
    /// Returns every key/value pair with `start <= key < end`, in key order.
    /// An empty `end` means unbounded above.
    ///
    /// # Errors
    /// Returns the underlying storage error on scan failure.
    fn range(&self, start: &[u8], end: &[u8])
        -> Result<KeyValueRows, rushdb_storage::StorageError>;
    /// The smallest key starting with `p`, or `None`. Snapshot and
    /// transaction readers answer with one bounded seek.
    ///
    /// # Errors
    /// Returns the underlying storage error on scan failure.
    fn first_key(&self, p: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        Ok(self.range_prefix(p)?.into_iter().next().map(|(key, _)| key))
    }
    /// The smallest key `k` with `start <= k < end` (an empty `end` is
    /// unbounded above), or `None`: one seek. Snapshot readers read one row.
    ///
    /// # Errors
    /// Returns the underlying storage error on scan failure.
    fn first_key_from(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        let mut found = None;
        self.for_each_key(start, end, &mut |key| {
            found = Some(key.to_vec());
            false
        })?;
        Ok(found)
    }
    /// At most `limit` rows starting with `p`, in key order. Snapshot readers
    /// read only those rows.
    ///
    /// # Errors
    /// Returns the underlying storage error on scan failure.
    fn range_prefix_limited(
        &self,
        p: &[u8],
        limit: usize,
    ) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        let mut rows = self.range_prefix(p)?;
        rows.truncate(limit);
        Ok(rows)
    }
    /// Calls `visit` with every key in `start <= key < end` (an empty `end`
    /// is unbounded above), in key order, until it returns `false`, without
    /// collecting the rows. Snapshot readers stream in bounded batches.
    ///
    /// # Errors
    /// Returns the underlying storage error on scan failure.
    fn for_each_key(
        &self,
        start: &[u8],
        end: &[u8],
        visit: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<(), rushdb_storage::StorageError> {
        for (key, _) in self.range(start, end)? {
            if !visit(&key) {
                break;
            }
        }
        Ok(())
    }
    /// [`Self::for_each_key`] with each row's value: `visit(key, value)`
    /// until it returns `false`, without collecting the rows.
    ///
    /// # Errors
    /// Returns the underlying storage error on scan failure.
    fn for_each_row(
        &self,
        start: &[u8],
        end: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<(), rushdb_storage::StorageError> {
        for (key, value) in self.range(start, end)? {
            if !visit(&key, &value) {
                break;
            }
        }
        Ok(())
    }
    /// The store's planner-statistics cache and this reader's snapshot index,
    /// when the reader is a committed snapshot.
    fn planner_stats(&self) -> Option<(&rushdb_storage::PlannerStats, u64)> {
        None
    }
}

impl TryRead for rushdb_storage::Txn<'_> {
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        rushdb_storage::Txn::get(self, k)
    }

    fn range_prefix(&self, p: &[u8]) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        rushdb_storage::Txn::range_prefix(self, p)
    }

    fn range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        rushdb_storage::Txn::range(self, start, end)
    }

    fn first_key(&self, p: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        Ok(self.keys_prefix_limited(p, 1)?.into_iter().next())
    }
}

impl TryRead for rushdb_storage::Store {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        self.read_view()?.get(key)
    }

    fn range_prefix(&self, prefix: &[u8]) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        self.read_view()?.range_prefix(prefix)
    }

    fn range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        self.read_view()?.range(start, end)
    }
}

pub mod try_access;

impl TryRead for rushdb_storage::ReadView {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        rushdb_storage::ReadView::get(self, key)
    }
    fn range_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, rushdb_storage::StorageError> {
        rushdb_storage::ReadView::range_prefix(self, prefix)
    }

    fn range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, rushdb_storage::StorageError> {
        rushdb_storage::ReadView::range(self, start, end)
    }

    fn range_prefix_limited(
        &self,
        p: &[u8],
        limit: usize,
    ) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut cursor = self.scan(
            rushdb_storage::ScanBounds::prefix(p),
            rushdb_storage::ScanProjection::KeyValue,
        )?;
        let mut rows = Vec::new();
        while rows.len() < limit {
            let batch = cursor.next_batch(limit - rows.len(), usize::MAX)?;
            for row in batch.rows {
                rows.push((row.key, row.value.unwrap_or_default()));
            }
            if batch.done {
                break;
            }
        }
        Ok(rows)
    }

    fn planner_stats(&self) -> Option<(&rushdb_storage::PlannerStats, u64)> {
        Some((self.planner_stats_cache(), self.commit_index()))
    }

    fn for_each_key(
        &self,
        start: &[u8],
        end: &[u8],
        visit: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<(), rushdb_storage::StorageError> {
        stream_rows(
            self,
            start,
            end,
            rushdb_storage::ScanProjection::Keys,
            &mut |key, _| visit(key),
        )
    }

    fn for_each_row(
        &self,
        start: &[u8],
        end: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<(), rushdb_storage::StorageError> {
        stream_rows(
            self,
            start,
            end,
            rushdb_storage::ScanProjection::KeyValue,
            visit,
        )
    }

    fn first_key(&self, p: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        let mut cursor = self.scan(
            rushdb_storage::ScanBounds::prefix(p),
            rushdb_storage::ScanProjection::Keys,
        )?;
        Ok(cursor
            .next_batch(1, usize::MAX)?
            .rows
            .into_iter()
            .next()
            .map(|row| row.key))
    }

    fn first_key_from(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        let bounds = rushdb_storage::ScanBounds {
            start: std::ops::Bound::Included(start.to_vec()),
            end: if end.is_empty() {
                std::ops::Bound::Unbounded
            } else {
                std::ops::Bound::Excluded(end.to_vec())
            },
        };
        let mut cursor = self.scan(bounds, rushdb_storage::ScanProjection::Keys)?;
        Ok(cursor
            .next_batch(1, usize::MAX)?
            .rows
            .into_iter()
            .next()
            .map(|row| row.key))
    }
}

impl TryRead for rushdb_storage::OverlayView {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        rushdb_storage::OverlayView::get(self, key)
    }

    fn range_prefix(&self, prefix: &[u8]) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        rushdb_storage::OverlayView::range_prefix(self, prefix)
    }

    fn range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        rushdb_storage::OverlayView::range(self, start, end)
    }
}

impl<R: TryRead + ?Sized> TryRead for &R {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        (*self).get(key)
    }

    fn range_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, rushdb_storage::StorageError> {
        (*self).range_prefix(prefix)
    }

    fn range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, rushdb_storage::StorageError> {
        (*self).range(start, end)
    }

    fn first_key(&self, p: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        (*self).first_key(p)
    }

    fn first_key_from(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        (*self).first_key_from(start, end)
    }

    fn range_prefix_limited(
        &self,
        p: &[u8],
        limit: usize,
    ) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        (*self).range_prefix_limited(p, limit)
    }

    fn planner_stats(&self) -> Option<(&rushdb_storage::PlannerStats, u64)> {
        (*self).planner_stats()
    }

    fn for_each_key(
        &self,
        start: &[u8],
        end: &[u8],
        visit: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<(), rushdb_storage::StorageError> {
        (*self).for_each_key(start, end, visit)
    }

    fn for_each_row(
        &self,
        start: &[u8],
        end: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<(), rushdb_storage::StorageError> {
        (*self).for_each_row(start, end, visit)
    }
}

impl<R: TryRead> TryRead for std::sync::Arc<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        self.as_ref().get(key)
    }
    fn range_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, rushdb_storage::StorageError> {
        self.as_ref().range_prefix(prefix)
    }
    fn range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, rushdb_storage::StorageError> {
        self.as_ref().range(start, end)
    }
    fn first_key(&self, p: &[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        self.as_ref().first_key(p)
    }
    fn first_key_from(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> {
        self.as_ref().first_key_from(start, end)
    }
    fn range_prefix_limited(
        &self,
        p: &[u8],
        limit: usize,
    ) -> Result<KeyValueRows, rushdb_storage::StorageError> {
        self.as_ref().range_prefix_limited(p, limit)
    }
    fn planner_stats(&self) -> Option<(&rushdb_storage::PlannerStats, u64)> {
        self.as_ref().planner_stats()
    }
    fn for_each_key(
        &self,
        start: &[u8],
        end: &[u8],
        visit: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<(), rushdb_storage::StorageError> {
        self.as_ref().for_each_key(start, end, visit)
    }
    fn for_each_row(
        &self,
        start: &[u8],
        end: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<(), rushdb_storage::StorageError> {
        self.as_ref().for_each_row(start, end, visit)
    }
}

/// Stream `start <= key < end` (empty `end`: unbounded) from a snapshot in
/// bounded batches, so no scan collects its rows (or hits the 64 MiB
/// materialization ceiling).
fn stream_rows(
    view: &rushdb_storage::ReadView,
    start: &[u8],
    end: &[u8],
    projection: rushdb_storage::ScanProjection,
    visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
) -> Result<(), rushdb_storage::StorageError> {
    let bounds = rushdb_storage::ScanBounds {
        start: std::ops::Bound::Included(start.to_vec()),
        end: if end.is_empty() {
            std::ops::Bound::Unbounded
        } else {
            std::ops::Bound::Excluded(end.to_vec())
        },
    };
    let mut cursor = view.scan(bounds, projection)?;
    loop {
        let batch = cursor.next_batch(4096, 8 * 1024 * 1024)?;
        for row in &batch.rows {
            if !visit(&row.key, row.value.as_deref().unwrap_or_default()) {
                return Ok(());
            }
        }
        if batch.done {

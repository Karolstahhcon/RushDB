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


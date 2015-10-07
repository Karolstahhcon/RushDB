//! Candidate ordinals for a search, in ascending ordinal order.
//!
//! A search without a usable index walks every record of the tenant or of
//! its labels. Those walks stream key batches from the snapshot instead of
//! collecting the population first, so a page can stop after its rows and
//! a count holds no ordinals at all.

use crate::planner::OwnerSet;
use crate::{QueryError, QuerySnapshot};

/// Rows fetched per scan batch.
const BATCH_ROWS: usize = 4096;
/// Bytes fetched per scan batch.
const BATCH_BYTES: usize = 8 * 1024 * 1024;

/// One key scan yielding the ordinal that ends each key.
pub(crate) struct OrdinalScan<'v> {
    cursor: rushdb_storage::ScanCursor<'v>,
    parse: fn(&[u8], usize) -> Option<u64>,
    prefix_len: usize,
    buffered: std::collections::VecDeque<u64>,
    done: bool,
}

impl<'v> OrdinalScan<'v> {
    fn new(
        snap: &'v QuerySnapshot,
        prefix: &[u8],
        parse: fn(&[u8], usize) -> Option<u64>,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            cursor: snap.scan(
                rushdb_storage::ScanBounds::prefix(prefix),
                rushdb_storage::ScanProjection::Keys,
            )?,
            parse,
            prefix_len: prefix.len(),
            buffered: std::collections::VecDeque::new(),
            done: false,
        })
    }

    fn next(&mut self, is_cancelled: &dyn Fn() -> bool) -> Result<Option<u64>, QueryError> {
        while self.buffered.is_empty() && !self.done {
            if is_cancelled() {
                return Err(QueryError::Cancelled);
            }
            let batch = self.cursor.next_batch(BATCH_ROWS, BATCH_BYTES)?;
            for row in batch.rows {
                self.buffered.push_back(
                    (self.parse)(&row.key, self.prefix_len)
                        .ok_or(rushdb_storage::StorageError::CorruptLog)?,

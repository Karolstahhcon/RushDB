//! Streamed relationship tables (`rushdb_edges.<type>`, SQL-4). Resolving a
//! table reads the type's columns and exact row count from the relationship
//! catalog (per-shape counts and property counters), without reading a
//! relationship; each scan then streams the type's relationships in bounded
//! batches. No edge table is held in memory, so the old
//! 100,000-relationship ceiling no longer applies; the request's memory and
//! deadline limits do.
//!
//! The relationship index holds every type in ordinal order, so a scan still
//! walks all relationships to return one type's; a per-type index would
//! make it proportional to the type (deferred).

use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow_json::reader::infer_json_schema_from_iterator;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Result as DFResult, Statistics};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion_expr::{Expr, TableType};
use rushdb_lmpg::reader::try_access as access;
use rushdb_lmpg::reconstruct::reconstruct_relationship;
use rushdb_storage::{ReadView, ScanBounds, ScanProjection, StorageError};
use rushdb_types::ids::OwnerKind;
use rushdb_types::ids::TenantId;
use rushdb_types::keys::{key, P_REL_BY_ORD};
use rushdb_types::model::{LogicalType, TypeState};
use rushdb_types::value::{ID_FIELD, SOURCE_FIELD, TARGET_FIELD, TYPE_FIELD};
use serde_json::Value as Json;

use crate::{dashed_id, SqlError};
use rushdb_lmpg::CatalogAccess;

/// Relationship index keys read per page.
const PAGE_KEYS: usize = 1024;
/// Rows per streamed batch.
const BATCH_ROWS: usize = 1024;

/// The relationships of one type, read page by page from the relationship
/// index (which holds every type, in ordinal order).
struct EdgeRows {
    snapshot: Arc<ReadView>,
    tenant: TenantId,
    rel_type: String,
    prefix: Vec<u8>,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    pending: VecDeque<Json>,
    done: bool,
    visible: CatalogAccess,
}

impl EdgeRows {
    fn new(
        snapshot: Arc<ReadView>,
        tenant: TenantId,
        rel_type: &str,
        visible: CatalogAccess,
    ) -> Self {
        let prefix = key(tenant, &[P_REL_BY_ORD]);
        let bounds = ScanBounds::prefix(&prefix);
        Self {
            snapshot,
            tenant,
            rel_type: rel_type.to_string(),
            prefix,
            lower: bounds.start,
            upper: bounds.end,
            pending: VecDeque::new(),
            done: false,
            visible,
        }
    }

    /// Under an enforced policy, a relationship is visible only when both
    /// endpoints' labels are allowed.
    fn endpoints_visible(&self, source: u64, target: u64) -> Result<bool, SqlError> {
        if !self.visible.enforced() {
            return Ok(true);
        }
        for ordinal in [source, target] {
            let header = access::record_by_ord(self.snapshot.as_ref(), self.tenant, ordinal)?
                .ok_or(StorageError::CorruptLog)?;
            if !self.visible.label(&header.label) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Reads the next page of the index into `pending`.
    fn fill(&mut self) -> Result<(), SqlError> {
        let mut cursor = self.snapshot.scan(
            ScanBounds {
                start: self.lower.clone(),
                end: self.upper.clone(),
            },
            ScanProjection::Keys,
        )?;
        let page = cursor.next_batch(PAGE_KEYS, 1024 * 1024)?;
        if let Some(last) = page.rows.last() {
            self.lower = Bound::Excluded(last.key.clone());
        }
        self.done = page.done || page.rows.is_empty();
        for member in &page.rows {
            let ordinal = ordinal_suffix(&member.key, &self.prefix)?;
            let header = access::relationship_by_ord(self.snapshot.as_ref(), self.tenant, ordinal)?
                .ok_or(StorageError::CorruptLog)?;
            if header.rel_type != self.rel_type
                || !self.endpoints_visible(header.source_ord, header.target_ord)?
            {
                continue;
            }
            let mut row = reconstruct_relationship(self.snapshot.as_ref(), self.tenant, ordinal)?
                .ok_or(StorageError::CorruptLog)?;
            if let Json::Object(object) = &mut row {
                for field in [ID_FIELD, SOURCE_FIELD, TARGET_FIELD] {
                    let id = object.remove(field).unwrap_or(Json::Null);
                    object.insert(field.into(), dashed_id(id));
                }
                self.pending.push_back(row);
            }
        }
        Ok(())
    }

    /// The next row, or `None` at the end.
    fn next_row(&mut self) -> Result<Option<Json>, SqlError> {
        while self.pending.is_empty() && !self.done {
            self.fill()?;
        }
        Ok(self.pending.pop_front())
    }
}

/// The ordinal at the end of a relationship-index key.
fn ordinal_suffix(storage_key: &[u8], prefix: &[u8]) -> Result<u64, StorageError> {
    if storage_key.len() != prefix.len() + 9
        || !storage_key.starts_with(prefix)
        || storage_key[prefix.len()] != b'/'
    {
        return Err(StorageError::CorruptLog);
    }
    Ok(u64::from_be_bytes(
        storage_key[prefix.len() + 1..]

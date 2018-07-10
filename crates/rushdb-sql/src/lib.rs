//! Relational SQL frontend backed by Apache `DataFusion`.
//!
//! Each execution exposes a tenant-scoped, immutable snapshot of LMPG records
//! as Arrow tables. Record labels are tables in the default schema and
//! relationship types are tables in the `rushdb_edges` schema. `DataFusion` owns
//! SQL parsing, planning, optimization, and execution; this crate only adapts
//! `RushDB`'s storage representation to Arrow.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_json::writer::{JsonArray, WriterBuilder};
use datafusion::common::ScalarValue;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::memory_pool::MemoryPool as _;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{ObjectName, Statement, TableFactor};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use futures::StreamExt;
use rushdb_lmpg::reader::try_access as access;
use rushdb_lmpg::{CatalogAccess, Lmpg};
use rushdb_storage::{resources::ByteReservation, ReadView, StorageError};
use rushdb_textsearch::TextIndexes;
use rushdb_types::keys::{key, P_REL_BY_ORD};
use rushdb_types::value::{ID_FIELD, LABEL_FIELD};
use serde_json::Value as Json;

/// A single query result row: column name to JSON value, in the
/// statement's column order.
pub type Row = serde_json::Map<String, Json>;

mod checked_sum;
mod dml;
mod edge_provider;
mod lazy_catalog;
mod scan_provider;
mod text_search;

fn ensure_sync_context() -> Result<(), SqlError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(SqlError::BlockingInRuntime);
    }
    Ok(())
}

/// Work counters emitted by the bounded `DataFusion` label scan. They are
/// request-local so a transport can attribute candidate and reconstruction
/// work without introducing a global metric label for every tenant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqlWorkCounters {
    /// Label-membership candidates visited.
    pub candidate_rows: u64,
    /// Canonical records reconstructed from selected properties.
    pub reconstructed_rows: u64,
    /// Candidates rejected by a pushed predicate.
    pub filtered_rows: u64,
    /// Membership posting bytes read from the bounded cursor.
    pub posting_bytes: u64,
}

thread_local! {
    static SQL_WORK: RefCell<SqlWorkCounters> = RefCell::new(SqlWorkCounters::default());
}

/// Reset request-local SQL scan counters before execution.
pub fn reset_work_counters() {
    SQL_WORK.with(|work| *work.borrow_mut() = SqlWorkCounters::default());
}

/// Take and reset request-local SQL scan counters after execution.
#[must_use]
pub fn take_work_counters() -> SqlWorkCounters {
    SQL_WORK.with(|work| std::mem::take(&mut *work.borrow_mut()))
}

pub(crate) fn record_work(update: impl FnOnce(&mut SqlWorkCounters)) {
    SQL_WORK.with(|work| update(&mut work.borrow_mut()));
}

#[cfg(test)]
mod pinned_tests;

/// Rows a SQL result, an eager mutation table or a relationship table may
/// hold, so an accidental unbounded query cannot build an unbounded
/// in-memory Arrow result. Label reads stream and never count against it
pub const MAX_SCAN_ROWS: usize = 100_000;
/// Small execution-pool allowance for `DataFusion`'s planner/operator state.
/// Retained result and Arrow-batch bytes are still checked against the exact
/// caller budget below; without this allowance even a one-byte result budget
/// fails while registering the schema before result admission runs.
const DATAFUSION_PHASE_OVERHEAD: usize = 16 * 1024 * 1024;

/// Wraps a [`datafusion::execution::memory_pool::GreedyMemoryPool`] (`DataFusion`'s
/// own enforcement, unchanged — `try_grow` still rejects over the real
/// `max_result_bytes` limit) while tracking the high-water mark of
/// `reserved()` across the query's execution. `MemoryPool` itself exposes
/// only the current `reserved()`, not a peak, so this is the minimal
/// wrapper needed to make `DataFusion`'s *real* memory use observable
/// (a measured number once a query has run, instead of a flat guess).
#[derive(Debug)]
struct PeakTrackingMemoryPool {
    // `TrackConsumersPool` wrapping `GreedyMemoryPool` is exactly what
    // `RuntimeEnvBuilder::with_memory_limit` built before this change (same
    // top-5 "largest consumers" detail in a resource-exhausted error
    // message); wrapping it here (instead of the other way around) keeps
    // that error-message behavior while still letting this struct's own
    // `Arc` handle be queried for `peak()` after the `RuntimeEnv` has taken
    // ownership of it as a trait object.
    inner: datafusion::execution::memory_pool::TrackConsumersPool<
        datafusion::execution::memory_pool::GreedyMemoryPool,
    >,
    peak: std::sync::atomic::AtomicUsize,
}

impl PeakTrackingMemoryPool {
    fn new(limit: usize) -> Self {
        Self {
            inner: datafusion::execution::memory_pool::TrackConsumersPool::new(
                datafusion::execution::memory_pool::GreedyMemoryPool::new(limit),
                std::num::NonZeroUsize::new(5).expect("5 is nonzero"),
            ),
            peak: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn peak(&self) -> usize {
        self.peak.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn observe(&self) {
        self.peak
            .fetch_max(self.inner.reserved(), std::sync::atomic::Ordering::Relaxed);
    }
}

impl std::fmt::Display for PeakTrackingMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl datafusion::execution::memory_pool::MemoryPool for PeakTrackingMemoryPool {
    fn name(&self) -> &'static str {
        "rushdb-peak-tracking"
    }

    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) {
        self.inner.grow(reservation, additional);
        self.observe();
    }

    fn shrink(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        shrink: usize,
    ) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.observe();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> datafusion::execution::memory_pool::MemoryLimit {
        self.inner.memory_limit()
    }
}

/// Errors produced while parsing, planning, or executing SQL over LMPG.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    /// A synchronous entry point was called from a Tokio runtime thread.
    #[error("synchronous SQL cannot run inside Tokio; use the async SQL API")]
    BlockingInRuntime,
    /// Reading the pinned storage snapshot failed.
    #[error("SQL storage read failed: {0}")]
    Storage(#[from] StorageError),
    /// The SQL text could not be parsed or bound.
    #[error("SQL parse failed: {0}")]
    Parse(String),
    /// A write statement was submitted to the read-only query path.
    #[error("SQL statement is not read-only: {0}")]
    ReadOnly(String),
    /// The statement is outside the supported atomic DML subset.
    #[error("SQL mutation is unsupported: {0}")]
    UnsupportedMutation(String),
    /// A row's stable `__id` is missing, malformed, or changed unexpectedly.
    #[error("SQL row identity is invalid: {0}")]
    InvalidIdentity(String),
    /// Another commit wrote the tenant after the statement's snapshot; the
    /// statement can be retried.
    #[error("SQL write conflict: {0}")]
    WriteConflict(String),
    /// Publishing the computed delta to LMPG failed.
    #[error("SQL mutation failed to commit atomically: {0}")]
    Commit(String),
    /// The snapshot exceeded the [`MAX_SCAN_ROWS`] safety limit.
    #[error("SQL snapshot exceeds the {MAX_SCAN_ROWS} row safety limit: {0}")]
    ResourceLimit(usize),
    /// A filter without a usable index would check more records of a label
    /// than the unindexed-walk limit allows (SQL-2, QRY-3; like records
    /// reads).
    #[error("unindexed walk refused: {0}")]
    UnindexedWalk(String),
    /// The statement needed more working memory than `query-memory-max`,
    /// tightened by the request, allows.
    #[error("query working set exceeded: the SQL statement needed more than {0} bytes")]
    MemoryLimit(usize),
    /// The caller cancelled or the deadline elapsed during execution. Polled
    /// between result batches, and by label scans between their batches, so
    /// a long scan stops promptly.
    #[error("SQL execution cancelled")]
    Cancelled,
    /// The `DataFusion` engine reported a planning or execution failure.
    #[error("SQL engine failed: {0}")]
    Engine(String),
    /// A `text_search(...)` call was invalid or its search failed (unknown
    /// index, bad arguments, unparsable query).
    #[error("SQL text search failed: {0}")]
    TextSearch(String),
}

/// Tenant-visible relational catalog returned by [`virtual_catalog`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    /// Record labels exposed as tables in the default schema, lowercased.
    pub tables: Vec<String>,
    /// Relationship types exposed in the `rushdb_edges` schema, lowercased.
    pub relationship_tables: Vec<String>,
}

/// Logical SQL type used in column and parameter descriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlType {
    /// Boolean value.
    Boolean,
    /// 16-bit signed integer.
    Int16,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 16-bit unsigned integer.
    UInt16,
    /// 32-bit unsigned integer.
    UInt32,
    /// 64-bit unsigned integer.
    UInt64,
    /// 32-bit floating-point value.
    Float32,
    /// 64-bit floating-point value.
    Float64,
    /// UTF-8 text.
    Utf8,
    /// Variable-length binary data.
    Binary,
    /// Calendar date.
    Date,
    /// Time of day or duration.
    Time,
    /// Date and time with optional time zone.
    Timestamp,
    /// Nested list, struct, map, or union value.
    Json,
    /// Type could not be mapped to a supported logical SQL type.
    Unknown,
}

/// One result column described by [`describe_sql`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlColumn {
    /// Column name.
    pub name: String,
    /// Logical SQL type of the column.
    pub data_type: SqlType,
    /// Whether the column may contain null values.
    pub nullable: bool,
}

/// Parsed description of a statement's bound parameters and result columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlDescription {
    /// Ordered by positional placeholder number, then named placeholder.
    pub parameters: Vec<(String, SqlType)>,
    /// Result columns, empty when the statement returns no rows.
    pub columns: Vec<SqlColumn>,
}

/// Result of executing one atomic DML statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlMutationResult {
    /// Rows produced by the `DataFusion` DML plan, typically the affected count.
    pub rows: Vec<Row>,
    /// Number of rows affected by the mutation.
    pub affected: u64,
    /// LMPG commit index published for the delta, or the snapshot version when
    /// the statement changed nothing.
    pub commit_index: u64,
}

/// Return the tenant-visible relational catalog without executing a query.
///
/// # Errors
///
/// Returns [`SqlError::Storage`] if the pinned catalog cannot be read.
pub fn virtual_catalog(lmpg: &Lmpg) -> Result<CatalogSnapshot, SqlError> {
    let snapshot = lmpg.store.read_view()?;
    let tables = access::labels(&snapshot, lmpg.tenant)?
        .into_iter()
        .map(|label| label.to_ascii_lowercase())
        .collect();
    let relationship_tables = relationship_types(lmpg, &snapshot)?
        .into_iter()
        .map(|rel_type| rel_type.to_ascii_lowercase())
        .collect();
    Ok(CatalogSnapshot {
        tables,
        relationship_tables,
    })
}

/// Execute a read-only SQL query and return its result rows.
///
/// # Errors
///
/// Returns [`SqlError::Parse`] for invalid SQL, [`SqlError::ReadOnly`] for a
/// write statement, [`SqlError::Storage`] for a storage read failure, or
/// [`SqlError::Engine`] for a `DataFusion` planning or execution failure.
pub fn execute_sql(lmpg: &Lmpg, sql: &str) -> Result<Vec<Row>, SqlError> {
    execute_sql_with_parameters(lmpg, sql, None)
}

/// Execute a read-only SQL query with optional bound parameters.
///
/// # Errors
///
/// Returns the same errors as [`execute_sql`], plus [`SqlError::Parse`] when
/// `parameters` is neither a JSON array nor object, or a bound value cannot be
/// converted to a supported scalar.
pub fn execute_sql_with_parameters(
    lmpg: &Lmpg,
    sql: &str,
    parameters: Option<&Json>,
) -> Result<Vec<Row>, SqlError> {
    execute_sql_with_parameters_budget(lmpg, sql, parameters, None)
}

/// Execute SQL with an optional cumulative response working-byte budget.
/// The budget is enforced while decoding `DataFusion` batches, before rows are
/// retained in the result vector.
///
/// # Errors
/// Returns the same errors as [`execute_sql_with_parameters`], including a
/// resource-limit error when the result exceeds `max_result_bytes`.
pub fn execute_sql_with_parameters_budget(
    lmpg: &Lmpg,
    sql: &str,
    parameters: Option<&Json>,
    max_result_bytes: Option<usize>,
) -> Result<Vec<Row>, SqlError> {
    ensure_sync_context()?;
    futures::executor::block_on(execute_sql_async_with_budget(
        lmpg,
        sql,
        parameters,
        max_result_bytes,
    ))
}

/// Execute SQL against a caller-owned pinned read view.
///
/// This is the engine transport path: the adapter must not open a second
/// snapshot while the surrounding request is using its execution context.
///
/// # Errors
/// Returns the same errors as [`execute_sql_with_parameters_budget`].
pub fn execute_sql_with_parameters_budget_in_view(
    lmpg: &Lmpg,
    snapshot: Arc<ReadView>,
    sql: &str,
    parameters: Option<&Json>,
    max_result_bytes: Option<usize>,
) -> Result<Vec<Row>, SqlError> {
    execute_sql_with_parameters_budget_in_view_cancellable(
        lmpg,
        snapshot,
        sql,
        parameters,
        max_result_bytes,
        &|| false,
        None,
    )
}

/// Cancellation-aware form of [`execute_sql_with_parameters_budget_in_view`].
/// `is_cancelled` is polled between result batches so a long analytical scan
/// stops promptly instead of only at the engine/transport boundary. Returns
/// [`SqlError::Cancelled`] when the predicate reports cancellation.
///
/// # Errors
///
/// Returns the same errors as [`execute_sql_with_parameters_budget_in_view`],
/// plus [`SqlError::Cancelled`].
pub fn execute_sql_with_parameters_budget_in_view_cancellable(
    lmpg: &Lmpg,
    snapshot: Arc<ReadView>,
    sql: &str,
    parameters: Option<&Json>,
    max_result_bytes: Option<usize>,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    text: Option<&Arc<TextIndexes>>,
) -> Result<Vec<Row>, SqlError> {
    ensure_sync_context()?;
    futures::executor::block_on(execute_sql_async_with_budget_in_view_cancellable(
        lmpg,
        snapshot,
        sql,
        parameters,
        max_result_bytes,
        is_cancelled,
        text,
    ))
}

/// Async form of [`execute_sql_with_parameters`].
///
/// # Errors
///
/// Returns the same errors as [`execute_sql_with_parameters`].
pub async fn execute_sql_async(
    lmpg: &Lmpg,
    sql: &str,
    parameters: Option<&Json>,
) -> Result<Vec<Row>, SqlError> {
    execute_sql_async_with_budget(lmpg, sql, parameters, None).await
}

/// Async form of [`execute_sql_with_parameters_budget`].
///
/// # Errors
/// Returns the same errors as [`execute_sql_with_parameters_budget`].
pub async fn execute_sql_async_with_budget(
    lmpg: &Lmpg,
    sql: &str,
    parameters: Option<&Json>,
    max_result_bytes: Option<usize>,
) -> Result<Vec<Row>, SqlError> {
    validate_read_only(sql)?;
    let snapshot = Arc::new(lmpg.store.read_view()?);
    execute_sql_async_with_budget_in_view(lmpg, snapshot, sql, parameters, max_result_bytes).await
}

/// Async form of [`execute_sql_with_parameters_budget_in_view`].
///
/// # Errors
/// Returns the same errors as [`execute_sql_with_parameters_budget_in_view`].
pub async fn execute_sql_async_with_budget_in_view(
    lmpg: &Lmpg,
    snapshot: Arc<ReadView>,
    sql: &str,
    parameters: Option<&Json>,
    max_result_bytes: Option<usize>,
) -> Result<Vec<Row>, SqlError> {
    execute_sql_async_with_budget_in_view_cancellable(

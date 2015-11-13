//! P09 lazy scan provider: a label is exposed to `DataFusion` as a streaming
//! `TableProvider` instead of an eagerly-materialized `MemTable`. Records are
//! reconstructed from the pinned `ReadView` on demand in bounded batches, so
//! catalog construction does NOT load the whole tenant and a selective query
//! materializes only what it scans.
//!
//! SQL-2: the table's schema comes from the property catalog, never from the
//! rows, and a filtered scan reads its candidates the way records reads do:
//! from the index plan when an index answers a pushed condition, from a
//! streamed walk of the label otherwise, refused like a records read when a
//! filter without a usable index would check more records than
//! `unindexed-walk-max-owners` allows. `DataFusion` re-applies every filter.
//!
//! Implementation: a `PartitionStream` feeds `StreamingTableExec`; the
//! `TableProvider::scan` returns that plan.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow_json::reader::infer_json_schema_from_iterator;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{Result as DFResult, ScalarValue, Statistics};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion_expr::TableType;
use datafusion_expr::{Expr, Operator, TableProviderFilterPushDown};
use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::{reconstruct, CatalogAccess, Lmpg};
use rushdb_query::{FieldPred, PlannedCandidates};
use rushdb_storage::{ReadView, ScanBounds, ScanProjection};
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::model::{LogicalType, TypeState};
use rushdb_types::value::ID_FIELD;
use serde_json::{Map, Value as Json};

use crate::SqlError;

/// Rows reconstructed per output batch (bounded working memory).
const BATCH_ROWS: usize = 256;
/// Rows per batch when no column is read: only ordinals are held.
const COUNT_BATCH_ROWS: usize = 4096;
/// Upper bound for the transient NDJSON buffer used to convert one bounded
/// Arrow batch. This is separate from the row ceiling so one oversized JSON
/// property cannot turn a single batch into an unbounded allocation.
const MAX_BATCH_JSON_BYTES: usize = 8 * 1024 * 1024;

/// A lazy table whose rows are the LMPG records of one label.
/// Column-vs-literal comparison pushed into the LMPG scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CmpOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// A conjunct of `column OP literal` filters pushed into the label scan.
type Pushdown = Vec<(String, CmpOp, datafusion::common::ScalarValue)>;

/// Translate `DataFusion` filter expressions into a scan pushdown: every
/// `column OP literal` conjunct of their `AND` trees. Other conjuncts are
/// left out; the candidates are planned from these, so they are a superset
/// of the filtered rows either way.
fn translate_filters(filters: &[Expr]) -> Pushdown {
    let mut out = Pushdown::new();
    for filter in filters {
        walk_filter(filter, &mut out);
    }
    out
}

/// Collect the `column OP literal` conjuncts of an `AND` tree; `true` when
/// the tree is nothing else.
fn walk_filter(e: &Expr, out: &mut Pushdown) -> bool {
    let Expr::BinaryExpr(b) = e else {
        return false;
    };
    if matches!(b.op, Operator::And) {
        let left = walk_filter(&b.left, out);
        let right = walk_filter(&b.right, out);
        return left && right;
    }
    let (Expr::Column(c), Expr::Literal(lit, _)) = (b.left.as_ref(), b.right.as_ref()) else {
        return false;
    };
    let op = match b.op {
        Operator::Eq => CmpOp::Eq,
        Operator::NotEq => CmpOp::Ne,
        Operator::Gt => CmpOp::Gt,
        Operator::GtEq => CmpOp::Gte,
        Operator::Lt => CmpOp::Lt,
        Operator::LtEq => CmpOp::Lte,
        _ => return false,
    };
    out.push((c.name.clone(), op, lit.clone()));
    true
}

/// The conjuncts of `filter` when the scan answers it in full: every one is
/// a comparison whose native predicate keeps exactly the rows SQL keeps
/// (`=` on a column of one kind of value, `>=`/`<=` on a numeric one), so
/// an exact index plan, or the strict check of [`eval_pushdown`], answers
/// it. `DataFusion` then drops the filter (`Exact`).
fn exact_conjuncts(filter: &Expr, columns: &HashMap<String, ColumnFacts>) -> Option<Pushdown> {
    let mut conjuncts = Pushdown::new();
    let exact = walk_filter(filter, &mut conjuncts)
        && conjuncts.iter().all(|(column, op, lit)| {
            column != ID_FIELD
                && matches!(op, CmpOp::Eq | CmpOp::Gte | CmpOp::Lte)
                && native_condition(column, *op, lit, columns).is_some()
        });
    exact.then_some(conjuncts)
}

/// Integers beyond this magnitude are not exact as an `f64` range bound.
const EXACT_F64_INTEGER: i64 = 1 << 53;

/// `i` as an `f64` range bound, when the conversion is exact.
#[expect(
    clippy::cast_precision_loss,
    reason = "only magnitudes up to 2^53, which an f64 holds exactly"
)]
fn exact_f64(i: i64) -> Option<f64> {
    (i.abs() <= EXACT_F64_INTEGER).then_some(i as f64)
}

/// A pushed conjunct as a native predicate whose index candidates hold every
/// row the conjunct keeps, or `None` when no such predicate is safe: the
/// column holds arrays, or a type the literal's comparison could coerce
/// (`'5'` against integers widened to text). Strict bounds widen to
/// inclusive ones; the rows are re-checked exactly.
fn native_condition(
    column: &str,
    op: CmpOp,
    lit: &ScalarValue,
    columns: &HashMap<String, ColumnFacts>,
) -> Option<rushdb_query::Expr> {
    if column == ID_FIELD {
        return match (op, lit) {
            (CmpOp::Eq, ScalarValue::Utf8(Some(id))) => {
                Some(rushdb_query::Expr::Field(FieldPred::Id(id.clone())))
            }
            _ => None,
        };
    }
    let facts = columns.get(column).filter(|facts| !facts.array)?;
    let only = |allowed: &[LogicalType]| {
        facts.types.iter().all(|state| match state {
            TypeState::Unresolved => true,
            TypeState::Known(ty) => allowed.contains(ty),
        })
    };
    let name = rushdb_query::normalize(column);
    let field = |pred: FieldPred| Some(rushdb_query::Expr::Field(pred));
    match lit {
        ScalarValue::Utf8(Some(text)) if op == CmpOp::Eq && only(&[LogicalType::Utf8]) => {
            field(FieldPred::Eq(name, Json::String(text.clone())))
        }
        ScalarValue::Boolean(Some(flag)) if op == CmpOp::Eq && only(&[LogicalType::Boolean]) => {
            field(FieldPred::Eq(name, Json::Bool(*flag)))
        }
        ScalarValue::Int64(Some(_)) | ScalarValue::Float64(Some(_))
            if only(&[LogicalType::Int64, LogicalType::Float64]) =>
        {
            let (number, bound) = match lit {
                ScalarValue::Int64(Some(i)) => (Json::Number((*i).into()), exact_f64(*i)),
                ScalarValue::Float64(Some(f)) => (
                    serde_json::Number::from_f64(*f).map(Json::Number)?,
                    Some(*f),
                ),
                _ => return None,
            };
            match op {
                CmpOp::Eq => field(FieldPred::Eq(name, number)),
                CmpOp::Gt | CmpOp::Gte => field(FieldPred::Gte(name, bound?)),
                CmpOp::Lt | CmpOp::Lte => field(FieldPred::Lte(name, bound?)),
                CmpOp::Ne => None,
            }
        }
        _ => None,
    }
}

/// Evaluate a pushdown against an externalized record (JSON object with the
/// scan's column values), strictly: a missing column, a null, or a value of
/// another kind than the literal never satisfies a comparison, as in SQL.
/// Only [`exact_conjuncts`] are checked here, whose columns hold one kind
/// of value.
fn eval_pushdown(row: &serde_json::Value, pushdown: &Pushdown) -> bool {
    let Some(obj) = row.as_object() else {
        return false;
    };
    pushdown.iter().all(|(column, op, lit)| {
        let Some(value) = obj.get(column) else {
            return false;
        };
        let cmp = match (value, lit) {
            (serde_json::Value::Number(_), datafusion::common::ScalarValue::Int64(Some(l))) => {
                let bound = serde_json::Value::Number((*l).into());
                rushdb_types::value::json_scalar_cmp(value, &bound)
                    .map(|ordering| compare_ordering(ordering, *op))
            }
            (serde_json::Value::Number(_), datafusion::common::ScalarValue::Float64(Some(l))) => {
                serde_json::Number::from_f64(*l)
                    .map(serde_json::Value::Number)
                    .and_then(|bound| {
                        rushdb_types::value::json_scalar_cmp(value, &bound)
                            .map(|ordering| compare_ordering(ordering, *op))
                    })
            }
            (serde_json::Value::String(s), datafusion::common::ScalarValue::Utf8(Some(l))) => {
                Some(match op {
                    CmpOp::Eq => s == l,
                    CmpOp::Ne => s != l,
                    CmpOp::Gt => s > l,
                    CmpOp::Gte => s >= l,
                    CmpOp::Lt => s < l,
                    CmpOp::Lte => s <= l,
                })
            }
            (serde_json::Value::Bool(b), datafusion::common::ScalarValue::Boolean(Some(l))) => {
                Some(match op {
                    CmpOp::Eq => *b == *l,
                    CmpOp::Ne => *b != *l,
                    _ => false,
                })
            }
            _ => None,
        };
        cmp.unwrap_or(false)
    })
}

fn compare_ordering(ordering: std::cmp::Ordering, op: CmpOp) -> bool {
    match op {
        CmpOp::Eq => ordering == std::cmp::Ordering::Equal,
        CmpOp::Ne => ordering != std::cmp::Ordering::Equal,
        CmpOp::Gt => ordering == std::cmp::Ordering::Greater,
        CmpOp::Gte => matches!(
            ordering,
            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
        ),
        CmpOp::Lt => ordering == std::cmp::Ordering::Less,
        CmpOp::Lte => matches!(
            ordering,
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal
        ),
    }
}

pub struct LabelTable {
    lmpg: Arc<Lmpg>,
    label: String,
    snapshot: Arc<ReadView>,
    schema: SchemaRef,
    /// The catalog's facts per column, for planning pushed conditions.
    columns: Arc<HashMap<String, ColumnFacts>>,
    /// Exact live row count for the label at this pinned view (from the derived
    /// label catalog). Surfaced to `DataFusion` as `Precision::Exact` so join and
    /// scan costing does not fall back to guesses.
    row_count: usize,
}

impl std::fmt::Debug for LabelTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabelTable")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

/// What the property catalog says about one column of a label.
#[derive(Debug, Default)]
struct ColumnFacts {
    /// The type states of the typed definitions the label's records carry.
    types: Vec<TypeState>,
    /// Some record of the label holds an array under the property.
    array: bool,
}

impl LabelTable {
    /// Build a table from catalog reads only: the label's columns from its
    /// property support counters and definitions, its row count from the
    /// label counter. No record is read, whatever the label's size.
    #[cfg(test)]
    pub fn try_new(
        lmpg: Arc<Lmpg>,
        label: String,
        snapshot: Arc<ReadView>,
    ) -> Result<Self, SqlError> {
        Self::try_new_visible(lmpg, label, snapshot, &CatalogAccess::default())
    }

    /// Like [`Self::try_new`], with only the columns `visible` allows
    ///: a hidden property is not a column, so no statement can read,
    /// filter on or name it.
    pub(crate) fn try_new_visible(
        lmpg: Arc<Lmpg>,
        label: String,
        snapshot: Arc<ReadView>,
        visible: &CatalogAccess,
    ) -> Result<Self, SqlError> {
        let tenant = lmpg.tenant;
        let (schema, columns) = label_columns(&snapshot, tenant, &label, &|property| {
            visible.property(&label, property)
        })?;
        let row_count = usize::try_from(rushdb_lmpg::derived::label_count(
            &snapshot, tenant, &label,
        )?)
        .unwrap_or(usize::MAX);
        Ok(Self {
            lmpg,
            label,
            snapshot,
            schema: Arc::new(schema),
            columns: Arc::new(columns),
            row_count,
        })
    }
}

/// The label's schema from the property catalog. Every typed
/// definition its records carry contributes one sample value of its type
/// (in an array when the label holds arrays under it), and the samples are
/// inferred by the same Arrow JSON rules as row data, so mixed types widen
/// exactly as they would over the rows. Columns follow property creation
/// order, then `__id`.
fn label_columns(
    snapshot: &ReadView,
    tenant: TenantId,
    label: &str,
    visible: &dyn Fn(&str) -> bool,
) -> Result<(Schema, HashMap<String, ColumnFacts>), SqlError> {
    let mut support =
        rushdb_lmpg::derived::prop_support_of_label(snapshot, tenant, OwnerKind::Record, label)?;
    support.retain(|(_, owners)| *owners > 0);
    support.sort_unstable();
    let mut first = Map::new();
    let mut extra: Vec<Json> = Vec::new();
    let mut columns: HashMap<String, ColumnFacts> = HashMap::new();
    for (pid, _) in support {
        let definition = try_access::definition(snapshot, tenant, pid)?
            .ok_or(rushdb_storage::StorageError::CorruptLog)?;
        let family = try_access::family(snapshot, tenant, definition.family_id)?
            .ok_or(rushdb_storage::StorageError::CorruptLog)?;
        if !visible(&family.display_name) {
            continue;
        }
        let array = rushdb_lmpg::derived::array_markers(snapshot, tenant, OwnerKind::Record, pid)?
            .1

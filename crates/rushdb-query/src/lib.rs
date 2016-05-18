//! Query planner and executor over the LMPG state.
//!
//! Turns a [`SearchRequest`] into an access path (`index_candidates`), executes
//! it against a pinned snapshot, and falls back to a full scan whenever a
//! predicate cannot be served exactly by the maintained indexes. Every index
//! path is required to return exactly what the forced-scan path returns; the
//! differential tests in `tests/planner_differential.rs` enforce that.
use rushdb_lmpg::reader::try_access as access;
use rushdb_lmpg::reconstruct;
use std::sync::Arc;
/// Caller-owned snapshot used for query execution and result shaping.
pub type QuerySnapshot = Arc<rushdb_storage::ReadView>;
use equality::{any_element, is_datetime, value_equals};
use rushdb_lmpg::{Lmpg, LmpgError};
use rushdb_types::model::{LogicalType, TypeState};
use rushdb_types::value::{
    infer_scalar, scalar_cmp as canonical_scalar_cmp, scalar_equal, Scalar, ID_FIELD,
};
use serde_json::{Map, Value as Json};
use thiserror::Error;

mod aggregate_budget;
mod aggregation;
mod cycle;
mod equality;
pub use aggregation::MAX_AGGREGATE_BUCKETS;
pub use aggregation::{
    aggregate, aggregate_in_view, aggregate_to_rows, aggregate_to_rows_in_view,
    aggregate_to_rows_in_view_with_budget, aggregate_with_cancel, aggregate_with_cancel_in_view,
    aggregate_with_cancel_in_view_with_budget,
};
#[cfg(test)]
use aggregation::{eval_row_expr, exact_f64, minmax};
mod page;
pub use page::effective_budget;
mod planned;
mod planner;
mod point_fields;
pub use point_fields::PointFields;
mod property_predicate;
pub use hex::hex_decode_16;
pub use index_access::explain_index;
pub use planned::{planned_candidates, PlannedCandidates};
use planner::index_candidates_with_budget;
pub use planner::{
    explain_cardinality, explain_range_selectivity, explain_walk, unindexed_walks_total,
    UNINDEXED_WALK_THRESHOLD,
};
pub use property_predicate::{matches_properties, parse_property_predicate};
pub mod datetime;
pub mod document;
mod hex;
mod index_access;
mod scan;
mod stats_cache;
pub use document::{ParsedQuery, QueryDocumentError};

/// A single-field predicate inside a `where` expression.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FieldPred {
    /// `$eq` equality; `$eq:null` is an explicit alias for the is-null state predicate.
    Eq(String, Json),
    /// `$ne` inequality; the field must be present for the predicate to match.
    Ne(String, Json),
    /// `$gt` numeric/datetime strictly-greater comparison.
    Gt(String, f64),
    /// `$gte` numeric/datetime greater-than-or-equal comparison.
    Gte(String, f64),
    /// `$lt` numeric/datetime strictly-less comparison.
    Lt(String, f64),
    /// `$lte` numeric/datetime less-than-or-equal comparison.
    Lte(String, f64),
    /// `$contains` case-insensitive substring match on a string.
    Contains(String, String),
    /// `$startsWith` case-insensitive prefix match on a string.
    StartsWith(String, String),
    /// `$exists` presence test; the flag selects present (`true`) or absent.
    Exists(String, bool),
    /// `$isNull`/`$eq:null` null-state test; the flag selects null (`true`) or non-null.
    IsNull(String, bool),
    /// `$in` membership against a set of literal values.
    In(String, Vec<Json>),
    /// `$nin` non-membership; the field must be present and match none of the values.
    Nin(String, Vec<Json>),
    /// `$endsWith` case-insensitive suffix match on a string.
    EndsWith(String, String),
    /// `$id` — match the record identity itself.
    Id(String),
    /// Datetime component predicate: `{ created: { $year: 2023 } }`.
    /// `which` = 0..6 (year/month/day/hour/minute/second/millisecond).
    DatetimeComponent {
        /// Normalized property name the component is read from.
        field: String,
        /// Component selector: 0=year, 1=month, 2=day, 3=hour, 4=minute, 5=second, 6=millisecond.
        which: u8,
        /// Expected component value.
        value: i64,
    },
    /// `$type` operator: "string" | "number" | "boolean" | "array" | "object"
    Type(String, String),
}

/// Boolean expression tree evaluated against a reconstructed record.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    /// Logical AND of every child.
    And(Vec<Expr>),
    /// Logical OR of every child.
    Or(Vec<Expr>),
    /// Logical XOR (`$xor`)
    Xor(Box<Expr>, Box<Expr>),
    /// Logical NOR (`$nor`)
    Nor(Vec<Expr>),
    /// Logical negation of the child.
    Not(Box<Expr>),
    /// A single field predicate.
    Field(FieldPred),
    /// Related-record traversal predicate (label-key blocks, `$relation`,
    /// `hops`, `$cycle`). Evaluated existentially over reachable targets.
    Related(RelatedExpr),
}

/// Traversal predicate mirroring the documented relationship syntax.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RelatedExpr {
    /// Labels an endpoint record must carry (empty = any label).
    pub target_labels: Vec<String>,
    /// Optional `$alias` binding for the endpoint, used by `$count`/`$sum`/`$collect`.
    pub alias: Option<String>,
    /// Relationship type filter; `None` matches every relation type.
    pub rel_type: Option<String>,
    /// Edge direction to traverse.
    pub direction: Direction,
    /// Variable-length traversal bounds `(min, max)`; absent = one hop.
    pub hops: Option<(u64, u64)>,
    /// `$cycle`: true when the traversal must return to the start record.
    pub cycle: bool,
    /// Predicates evaluated on the endpoint record (may nest further
    /// `RelatedExpr` children for multi-level blocks).
    pub filter: Option<Box<Expr>>,
}

/// A root-bound traversal branch declared under `related` or a `where` alias.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RelatedSpec {
    /// Alias the branch is bound to (without leading `$`).
    pub alias: Option<String>,
    /// Relationship type filter for the final step; `None` matches any type.
    pub rel_type: Option<String>,
    /// Edge direction for the final step.
    pub direction: Direction,
    /// Labels the final-step endpoint must carry (empty = any).
    pub target_labels: Vec<String>,
    /// The full traversal path from the root record to this spec's level
    /// (ancestors first; the last step is the spec's own level). Each step
    /// carries that level's label/rel-type/direction and endpoint predicates,
    /// so alias `$count`/`$sum`/`$collect` match the reference: multi-hop
    /// aliases traverse every level and apply each level's `where` filter.
    pub steps: Vec<TraversalStep>,
}

/// One level of an alias traversal path (see [`RelatedSpec::steps`]).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TraversalStep {
    /// Labels the endpoint at this level must carry (empty = any label).
    pub target_labels: Vec<String>,
    /// Relationship type filter at this level; `None` matches any type.
    pub rel_type: Option<String>,
    /// Edge direction at this level.
    pub direction: Direction,
    /// Endpoint predicates evaluated on the record at this level (nested label
    /// blocks are separate steps, not part of this filter).
    pub filter: Option<Expr>,
}

/// `$collect` at the root binding grain (document 19): one declared step
/// per level, batched typed-adjacency collection, optional uniqueness and
/// local slice. Output attaches to each returned record under `alias`.
///
/// Two forms: **alias-based** (`from` in the docs) resolves `rel_type`/
/// `direction`/`target_labels` from the matching `where` traversal alias, and
/// projects whole records unless `fields` is set; **label-based** (`label` in
/// the docs) traverses inline (direction defaults to outgoing), supports a
/// `where` filter, an output-key→field projection map, and **nested** collects
/// evaluated recursively on each collected record (hierarchical output).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CollectSpec {
    /// Output key the collected array is attached under.
    pub alias: String,
    /// Relationship type filter for direct-adjacency collects.
    pub rel_type: Option<String>,
    /// Edge direction for direct-adjacency collects.
    pub direction: Direction,
    /// Labels a collected record must carry (empty = any label).
    pub target_labels: Vec<String>,
    /// Alias-based (`from`) collects carry the full traversal path to the
    /// alias, including each level's `where` predicates. Empty for
    /// label-based collects (direct adjacency).
    pub steps: Vec<TraversalStep>,
    /// `(output_key, source_field)` projection; empty = whole record.
    pub fields: Vec<(String, String)>,
    /// Nested (hierarchical) collects evaluated on each collected record.
    pub nested: Vec<CollectSpec>,
    /// `where` filter evaluated on each collected record (label-based).
    pub filter: Option<Expr>,
    /// ORDER BY (field, desc) applied to the collected array.
    pub order_by: Vec<(String, bool)>,
    /// When true, deduplicate collected records by identity.
    pub unique: bool,
    /// Number of collected records to skip after ordering.
    pub skip: u64,
    /// Maximum number of collected records to return.
    pub limit: Option<u64>,
}

/// Direction of an edge traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Direction {
    /// Follow outgoing edges (root to target).
    Outgoing,
    /// Follow incoming edges (target to root).
    Incoming,
    /// Follow edges in either direction.
    Both,
}

/// Calendar/clock unit for a fixed-width time bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimeUnit {
    /// One-second buckets.
    Seconds,
    /// One-minute buckets.
    Minutes,
    /// One-hour buckets.
    Hours,
    /// One-day buckets.
    Days,
    /// One-week buckets.
    Weeks,
    /// One-month buckets (calendar-aligned).
    Months,
    /// Three-month buckets (calendar-aligned).
    Quarters,
    /// One-year buckets (calendar-aligned).
    Years,
}

impl TimeUnit {
    fn millis(self) -> i64 {
        match self {
            TimeUnit::Seconds => 1_000,
            TimeUnit::Minutes => 60_000,
            TimeUnit::Hours => 3_600_000,
            TimeUnit::Days => 86_400_000,
            TimeUnit::Weeks => 7 * 86_400_000,
            TimeUnit::Months => 30 * 86_400_000,
            TimeUnit::Quarters => 90 * 86_400_000,
            TimeUnit::Years => 365 * 86_400_000,
        }
    }

    /// Parse a time unit from its documented name (singular or plural).
    ///
    /// # Errors
    ///
    /// Returns [`crate::QueryDocumentError::Parse`] when `name` is not a
    /// recognized time unit.
    pub fn from_name(name: &str) -> Result<TimeUnit, crate::QueryDocumentError> {
        Ok(match name {
            "second" | "seconds" => TimeUnit::Seconds,
            "minute" | "minutes" => TimeUnit::Minutes,
            "hour" | "hours" => TimeUnit::Hours,
            "day" | "days" => TimeUnit::Days,
            "week" | "weeks" => TimeUnit::Weeks,
            "month" | "months" => TimeUnit::Months,
            "quarter" => TimeUnit::Quarters,
            "year" | "years" => TimeUnit::Years,
            other => {
                return Err(crate::QueryDocumentError::Parse(format!(
                    "unknown time unit `{other}`"
                )));
            }
        })
    }

    pub(crate) fn unit_str(self) -> &'static str {
        match self {
            TimeUnit::Seconds => "seconds",
            TimeUnit::Minutes => "minutes",
            TimeUnit::Hours => "hours",
            TimeUnit::Days => "days",
            TimeUnit::Weeks => "weeks",
            TimeUnit::Months => "months",
            TimeUnit::Quarters => "quarters",
            TimeUnit::Years => "years",
        }
    }
}

/// Fixed-size calendar bucket over an epoch-millisecond field. Non-datetime
/// or missing input collapses to the documented MISSING state key.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TimeBucketSpec {
    /// Property whose epoch-millisecond value is bucketed.
    pub field: String,
    /// Calendar/clock unit of the bucket.
    pub unit: TimeUnit,
    /// Number of `unit`s per bucket (always at least 1).
    pub size: u64,
}

/// Maximum accepted `$precision` for `$avg`. `f64` carries ~15 significant
/// decimal digits; larger values would only amplify rounding noise (and
/// `10f64.powi` overflows past ~308). Parsed values are clamped to this.
pub const MAX_AVG_PRECISION: u32 = 15;

impl TimeBucketSpec {
    /// A bucket of exactly one `unit`.
    pub fn fixed(field: impl Into<String>, unit: TimeUnit) -> Self {
        TimeBucketSpec {
            field: field.into(),
            unit,
            size: 1,
        }
    }

    /// A bucket spanning `size` `unit`s.
    ///
    /// # Panics
    ///
    /// Panics when `size` is zero.
    pub fn sized(field: impl Into<String>, unit: TimeUnit, size: u64) -> Self {
        assert!(size > 0, "timeBucket size must be positive");
        TimeBucketSpec {
            field: field.into(),
            unit,
            size,
        }
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "epoch-ms is a bounded timestamp; the saturating float-to-int cast matches the reference calendar flooring"
    )]
    fn floor_millis(&self, epoch_ms: f64) -> i64 {
        let ms = epoch_ms as i64;
        // calendar-aware flooring for Months/Years (fixed-width would not
        // align to calendar boundaries)

        match self.unit {
            TimeUnit::Months => rushdb_types::datetime::start_of_month(ms),
            TimeUnit::Quarters => rushdb_types::datetime::start_of_quarter(ms),
            TimeUnit::Years => rushdb_types::datetime::start_of_year(ms),
            _ => {
                // Defensive even though the parser rejects `size == 0`: a
                // directly constructed spec must not divide by zero or wrap.
                let size = i64::try_from(self.size).unwrap_or(i64::MAX).max(1);
                let width = self.unit.millis().saturating_mul(size).max(1);
                ms.div_euclid(width).saturating_mul(width)
            }
        }
    }
}

/// Offset/limit pagination applied after matching and ordering.
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Pagination {
    /// Number of matching roots to skip.
    pub skip: u64,
    /// Maximum number of matching roots to return; `None` = unbounded.
    pub limit: Option<u64>,
}

/// A records-perspective search over one committed snapshot.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SearchRequest {
    /// Labels a root record must carry; empty matches every label.
    #[serde(default)]
    pub labels: Vec<String>,
    /// Boolean predicate the root record must satisfy.
    #[serde(default, rename = "where")]
    pub where_: Option<Expr>,
    /// Independent traversal branches; every spec filters existentially so
    /// independent one-to-many branches never multiply the root grain.
    #[serde(default)]
    pub related: Vec<RelatedSpec>,
    /// Per-root `$collect` output shapes attached to each returned record.
    #[serde(default)]
    pub collects: Vec<CollectSpec>,
    /// Offset/limit applied after matching and ordering.
    #[serde(default)]
    pub pagination: Pagination,
    /// ORDER BY (field, desc) applied after matching, before pagination.
    #[serde(default)]
    pub order_by: Vec<(String, bool)>,
    /// Test/oracle switch: when true, the index access path is bypassed and
    /// the executor performs a full scan, letting differential tests compare
    /// the two paths on identical input.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub force_scan: bool,
}

/// Errors returned by query planning and execution.
#[derive(Debug, Error)]
pub enum QueryError {
    /// A storage read or snapshot error.
    #[error("storage error: {0}")]
    Storage(#[from] rushdb_storage::StorageError),
    /// An engine (LMPG) error.
    #[error("engine error: {0}")]
    Engine(#[from] LmpgError),
    /// A predicate could not be evaluated (e.g. divide by zero, unresolved ref).
    #[error("invalid predicate on `{0}`")]
    InvalidPredicate(String),
    /// The cooperative cancellation probe requested a stop.
    #[error("query cancelled")]
    Cancelled,
    /// A declared resource budget (e.g. distinct aggregate buckets) was exceeded.
    #[error("resource exhausted: {0}")]
    ResourceLimit(String),
    /// A filter without a usable index would check more records than
    /// `unindexed-walk-max-owners` allows.
    #[error("unindexed walk refused: {0}")]
    UnindexedWalk(String),
}

/// One page of reconstructed records plus the pre-pagination match total.
#[derive(Debug, Clone)]
pub struct Page {
    /// Number of distinct roots matched before pagination.
    pub total_matched: u64,
    /// Reconstructed records in this page, in query order.
    pub records: Vec<Json>,
}

/// Records-perspective execution over one committed snapshot. Missing
/// property = no occurrence; explicit null = live payload-free occurrence.
///
/// # Errors
///
/// Propagates storage/engine read errors and predicate evaluation failures as
/// [`QueryError`].
pub fn find_records(lmpg: &Lmpg, req: &SearchRequest) -> Result<Page, QueryError> {
    find_records_with_cancel(lmpg, req, &|| false)
}

/// Records-perspective execution with a cooperative cancellation probe.
/// The probe is checked inside scan and reconstruction loops so a cancelled
/// stream stops consuming CPU instead of merely discarding its response.
///
/// # Errors
///
/// Returns [`QueryError::Cancelled`] when the probe requests a stop, and
/// propagates storage/engine read errors and predicate evaluation failures.
pub fn find_records_with_cancel(
    lmpg: &Lmpg,
    req: &SearchRequest,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<Page, QueryError> {
    let snap = Arc::new(lmpg.store.read_view()?);
    find_records_in_view(lmpg, req, is_cancelled, snap)
}

/// Every ordinal matching `req`, in match order (`order_by` applied).
///
/// Pagination is ignored: callers apply `skip`/`limit` themselves. The index
/// bounded-page path is not used, so a delete can snapshot the full match set
/// once instead of re-scanning after every page. Page JSON is not built. The
/// working set is capped by `query-memory-max` ([`working_set_budget`]).
///
/// # Errors
///
/// Returns [`QueryError::Cancelled`] when the probe requests a stop,
/// [`QueryError::ResourceLimit`] when the working set exceeds
/// `query-memory-max`, and propagates storage/engine read errors and
/// predicate evaluation failures.
#[expect(
    clippy::needless_pass_by_value,
    reason = "callers hand over a freshly pinned view, matching find_records_in_view"
)]
pub fn matching_ordinals(
    lmpg: &Lmpg,
    req: &SearchRequest,
    is_cancelled: &dyn Fn() -> bool,
    snap: QuerySnapshot,
) -> Result<Vec<u64>, QueryError> {
    let is_cancelled: &dyn Fn() -> bool = &cancel_probe(lmpg, is_cancelled);
    if !snap.belongs_to(&lmpg.store) {
        return Err(rushdb_storage::StorageError::InvalidRead(
            "query view belongs to another Store".into(),
        )
        .into());
    }
    if is_cancelled() {
        return Err(QueryError::Cancelled);
    }
    let mut full = req.clone();
    full.pagination = Pagination::default();
    Ok(run_matches(lmpg, &full, is_cancelled, &snap, usize::MAX, None)?.retained)
}

/// The matches a search keeps, and how many there are in all.
struct Matches {
    /// The first `keep` matches in query order.
    retained: Vec<u64>,
    /// Every match, retained or not.
    total: u64,
}

/// One pass over the candidates in ascending ordinal order.
///
/// Keeps the first `keep` matches in query order and counts the rest
/// without keeping them. Without `order_by` the pass stops once `keep`
/// matches are held and the total is already known: from the caller (a
/// cursor continuation on its pinned view), from counters (no predicate),
/// or from the index plan's owner set when the index answered the whole
/// predicate. With `order_by` it keeps the best `keep` rows seen so far.
/// The working set (the candidate owner set, retained ordinals, ordering
/// rows and the record being evaluated) is charged against
/// `query-memory-max`.
fn run_matches(
    lmpg: &Lmpg,
    req: &SearchRequest,
    is_cancelled: &dyn Fn() -> bool,
    snap: &QuerySnapshot,
    keep: usize,
    known_total: Option<u64>,
) -> Result<Matches, QueryError> {
    check_walk_budget(lmpg, snap, req)?;
    let mut working = WorkingSet {
        limit: working_set_budget(lmpg).map(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX)),
        charged: 0,
    };
    let CandidateSource {
        mut candidates,
        exact,
        known,
        predicate,
    } = candidate_source(lmpg, req, is_cancelled, snap, known_total, &mut working)?;
    let ctx = EvalCtx {
        lmpg,
        snap: snap.clone(),
        is_cancelled: Some(is_cancelled),
        cycles: crate::cycle::CycleGraphs::default(),
        declared_types: std::cell::RefCell::default(),
    };
    let mut session = reconstruct::ReconstructSession::new(snap, lmpg.tenant);
    let order_fields = if req.order_by.is_empty() {
        None
    } else {
        PointFields::resolve(
            snap,
            lmpg.tenant,
            req.order_by.iter().map(|(f, _)| f.as_str()),
        )?
    };
    let predicate_fields = match &predicate {
        Some(expr) => PointFields::for_predicate(snap, lmpg.tenant, expr)?,
        None => None,
    };
    let mut retained: Vec<u64> = Vec::new();
    // Ordered scans hold `(ordinal, row, row bytes)`, sorted and cut back to
    // `keep` whenever they double, so memory follows `keep`, not the matches.
    let mut ordered: Vec<OrderRow> = Vec::new();
    let mut ordered_bytes: Vec<u64> = Vec::new();
    let mut total: u64 = 0;
    while let Some(ord) = candidates.next(is_cancelled)? {
        if is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        if !exact
            && !is_match(
                &ctx,
                req,
                predicate.as_ref(),
                predicate_fields.as_ref(),
                &mut session,
                &mut working,
                ord,
            )?
        {
            continue;
        }
        total += 1;
        if req.order_by.is_empty() {
            if retained.len() < keep {
                working.charge(8, true)?;
                retained.push(ord);
            } else if known.is_some() {
                break;
            }
            continue;
        }
        let row = read_row(order_fields.as_ref(), &mut session, snap, lmpg.tenant, ord)?;
        let bytes =
            serde_json::to_vec(&row).map_or(0, |v| u64::try_from(v.len()).unwrap_or(u64::MAX));
        working.charge(bytes, true)?;
        ordered.push((ord, row));
        ordered_bytes.push(bytes);
        if ordered.len() >= keep.saturating_mul(2).max(64) {
            working.charged = working.charged.saturating_sub(cut_ordered(
                &mut ordered,
                &mut ordered_bytes,
                req,
                keep,
            ));
        }
    }
    if !req.order_by.is_empty() {
        cut_ordered(&mut ordered, &mut ordered_bytes, req, keep);
        retained = ordered.into_iter().map(|(ord, _)| ord).collect();
    }
    Ok(Matches {
        retained,
        total: known.unwrap_or(total),
    })
}

/// The bytes a search holds at once, against `query-memory-max`.
struct WorkingSet {
    limit: Option<u64>,
    charged: u64,
}

impl WorkingSet {
    /// Admit `bytes`; `retained` bytes stay charged, others (a record that
    /// only feeds the predicate) only need to fit in the remaining room.
    fn charge(&mut self, bytes: u64, retained: bool) -> Result<(), QueryError> {
        if let Some(limit) = self.limit {
            if self.charged.saturating_add(bytes) > limit {
                return Err(QueryError::ResourceLimit(format!(
                    "query working set exceeded {limit} bytes"
                )));
            }
            if retained {
                self.charged += bytes;
            }
        }
        Ok(())
    }
}

/// Whether candidate `ord` is a live match: the record exists, `predicate`
/// holds on it, and every relationship branch binds.
fn is_match(
    ctx: &EvalCtx<'_>,
    req: &SearchRequest,
    predicate: Option<&Expr>,
    fields: Option<&PointFields>,
    session: &mut reconstruct::ReconstructSession<'_, QuerySnapshot>,
    working: &mut WorkingSet,
    ord: u64,
) -> Result<bool, QueryError> {
    let (snap, tenant) = (&ctx.snap, ctx.lmpg.tenant);
    if access::record_by_ord(snap, tenant, ord)?.is_none() {
        return Ok(false);
    }
    if let Some(expr) = predicate {
        let obj = read_row(fields, session, snap, tenant, ord)?;
        let bytes =
            serde_json::to_vec(&obj).map_or(0, |v| u64::try_from(v.len()).unwrap_or(u64::MAX));
        working.charge(bytes, false)?;
        if !eval_expr_ctx(ctx, expr, &obj)? {
            return Ok(false);
        }
    }
    for spec in &req.related {
        if !related_exists(snap, tenant, ord, spec)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The properties `fields` names, by point reads, or the rebuilt record.
fn read_row(
    fields: Option<&PointFields>,
    session: &mut reconstruct::ReconstructSession<'_, QuerySnapshot>,
    snap: &QuerySnapshot,
    tenant: rushdb_types::ids::TenantId,
    ord: u64,
) -> Result<Map<String, Json>, QueryError> {
    match fields {
        Some(fields) => fields.row(snap, tenant, ord),
        None => Ok(session
            .reconstruct_record(ord)?
            .and_then(|record| match record {
                Json::Object(map) => Some(map),
                _ => None,
            })
            .unwrap_or_default()),
    }
}

/// Where a search's candidates come from and what is left to check.
struct CandidateSource<'v> {
    candidates: scan::Candidates<'v>,
    /// Every candidate is a live match: neither the predicate nor the
    /// record's existence is checked per candidate.
    exact: bool,
    /// The total, when known without counting.
    known: Option<u64>,
    /// What is left to check per candidate: the conjuncts the owner set does
    /// not answer, or the whole `where` on a walk.
    predicate: Option<Expr>,
}

/// The index plan's owner set (charged to `working`), or a walk of the
/// tenant's or labels' records.
fn candidate_source<'v>(
    lmpg: &Lmpg,
    req: &SearchRequest,
    is_cancelled: &dyn Fn() -> bool,
    snap: &'v QuerySnapshot,
    known_total: Option<u64>,
    working: &mut WorkingSet,
) -> Result<CandidateSource<'v>, QueryError> {
    if let Some(plan) =
        index_candidates_with_budget(lmpg, snap, req, working_set_budget(lmpg), is_cancelled)?
    {
        working.charge(

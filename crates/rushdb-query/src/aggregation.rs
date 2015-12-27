//! Budgeted grouping, aggregate evaluation, and result projection.

use super::{
    aggregate_budget, alias_field_values, attach_collects_with_budget, binding_roots_with_cancel,
    canonical_scalar_cmp, count_related, infer_scalar, json_value_cmp, normalize, reconstruct,
    related_specs_from_where, resolve_field, resolve_field_numeric, AggregateRequest, Arc,
    GroupBucket, Json, Lmpg, Map, OutputExpr, Pagination, QueryError, QuerySnapshot, Scalar,
    SearchRequest, MISSING_SENTINEL,
};

/// Branch-grain aggregation over a `BindingRelation` of distinct roots:
/// one root contributes exactly one binding row, independent branches are
/// filtered existentially rather than multiplied into flat rows.
///
/// # Errors
///
/// Propagates storage/engine read errors and predicate evaluation failures as
/// [`QueryError`].
pub fn aggregate(lmpg: &Lmpg, req: &AggregateRequest) -> Result<Vec<GroupBucket>, QueryError> {
    aggregate_with_cancel(lmpg, req, &|| false).map(|(buckets, _)| buckets)
}

/// Maximum number of distinct buckets a single aggregation may create before
/// it is refused with [`QueryError::ResourceLimit`].
pub const MAX_AGGREGATE_BUCKETS: usize = 1_000;

/// Aggregate execution with cooperative cancellation at root and bucket
/// boundaries. Returns the completed buckets plus the total number of matched
/// roots (the `total` reported by the data plane, matching a search result's
/// total).
///
/// # Errors
///
/// Returns [`QueryError::Cancelled`] when the probe requests a stop, and
/// [`QueryError::ResourceLimit`] when more than [`MAX_AGGREGATE_BUCKETS`]
/// distinct groups would be created. Storage/engine and predicate errors are
/// propagated from the underlying reads.
///
/// # Panics
///
/// Panics only if a bucket just inserted by `ensure_bucket` cannot be
/// retrieved, which is unreachable.
pub fn aggregate_with_cancel(
    lmpg: &Lmpg,
    req: &AggregateRequest,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<(Vec<GroupBucket>, u64), QueryError> {
    let snap = Arc::new(lmpg.store.read_view()?);
    aggregate_with_cancel_in_view(lmpg, req, is_cancelled, snap)
}

/// Aggregate execution over a caller-owned pinned read view. Embedded
/// engines use this to keep every phase of a request on one snapshot and to
/// apply the same deadline/budget context as the transport path.
///
/// # Errors
/// Propagates storage, predicate, cancellation and aggregate resource errors.
pub fn aggregate_in_view(
    lmpg: &Lmpg,
    req: &AggregateRequest,
    snap: QuerySnapshot,
) -> Result<(Vec<GroupBucket>, u64), QueryError> {
    aggregate_with_cancel_in_view(lmpg, req, &|| false, snap)
}

/// Execute an aggregate on the supplied view using the configured query budget.
///
/// # Errors
/// Returns storage, predicate, cancellation, or resource errors.
pub fn aggregate_with_cancel_in_view(
    lmpg: &Lmpg,
    req: &AggregateRequest,
    is_cancelled: &dyn Fn() -> bool,
    snap: QuerySnapshot,
) -> Result<(Vec<GroupBucket>, u64), QueryError> {
    aggregate_with_cancel_in_view_with_budget(lmpg, req, is_cancelled, snap, None)
}

/// Execute an aggregate with one caller budget across working state and groups.
/// An explicit zero budget rejects allocations; an absent budget uses storage configuration.
///
/// # Errors
/// Returns storage, predicate, cancellation, or resource errors.
pub fn aggregate_with_cancel_in_view_with_budget(
    lmpg: &Lmpg,
    req: &AggregateRequest,
    is_cancelled: &dyn Fn() -> bool,
    snap: QuerySnapshot,
    work_budget: Option<usize>,
) -> Result<(Vec<GroupBucket>, u64), QueryError> {
    let is_cancelled: &dyn Fn() -> bool = &crate::cancel_probe(lmpg, is_cancelled);
    if !snap.belongs_to(&lmpg.store) {
        return Err(rushdb_storage::StorageError::InvalidRead(
            "aggregate view belongs to another Store".into(),
        )
        .into());
    }
    if is_cancelled() {
        return Err(QueryError::Cancelled);
    }
    let configured = lmpg
        .limits()
        .query_memory_max_bytes()
        .map(|bytes| usize::try_from(bytes).unwrap_or(usize::MAX));
    let budget = match (work_budget, configured) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    aggregate_budget::check(budget, std::mem::size_of::<GroupBucket>())?;
    let snap = match budget {
        Some(bytes) => snap.with_materialization_limit(bytes / 16),
        None => snap,
    };
    aggregate_budgeted(lmpg, req, is_cancelled, snap, budget)
        .map_err(aggregate_budget::resource_error)
}

#[expect(
    clippy::too_many_lines,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::needless_pass_by_value,
    reason = "aggregate accumulation keeps one live budget and pinned snapshot"
)]
fn aggregate_budgeted(
    lmpg: &Lmpg,
    req: &AggregateRequest,
    is_cancelled: &dyn Fn() -> bool,
    snap: QuerySnapshot,
    budget: Option<usize>,
) -> Result<(Vec<GroupBucket>, u64), QueryError> {
    let page_req = SearchRequest {
        labels: req.labels.clone(),
        where_: req.where_.clone(),
        related: req.related.clone(),
        collects: Vec::new(),
        pagination: Pagination::default(),
        order_by: Vec::new(),
        force_scan: false,
    };
    // Count-only fast path: a single global `$count: "*"` with no grouping or
    // other metrics needs only the match count. A single unscoped label is
    // answered straight from the maintained label catalog (O(labels)); other
    // shapes fall through to the ordinal matcher below, which never
    // reconstructs records to produce a count.
    if is_count_only(req) {
        if let Some(total) = catalog_count_total(lmpg, &page_req, &snap)? {
            let mut buckets = count_only_bucket(total);
            apply_aggregate_pagination(&mut buckets, req);
            return Ok((buckets, total));
        }
    }
    let candidates: Vec<u64> =
        binding_roots_with_cancel(lmpg, &page_req, is_cancelled, &snap, budget)?;
    let candidate_bytes = candidates
        .capacity()
        .saturating_mul(std::mem::size_of::<u64>());
    aggregate_budget::check(budget, candidate_bytes)?;
    let total_matched = candidates.len() as u64;
    if is_count_only(req) {
        let mut buckets = count_only_bucket(total_matched);
        apply_aggregate_pagination(&mut buckets, req);
        return Ok((buckets, total_matched));
    }
    // Aggregate alias targets come from the `where` traversal blocks.
    let alias_specs = related_specs_from_where(req.where_.as_ref());
    let pure_projection = !req.plain_fields.is_empty()
        && req.group_by.is_empty()
        && req.sums.is_empty()
        && req.counts.is_empty()
        && req.mins.is_empty()
        && req.maxs.is_empty()
        && req.outputs.is_empty()
        && req.expr_sums.is_empty()
        && req.expr_avgs.is_empty()
        && req.avg_terms.is_empty()
        && req.time_bucket.is_none();

    // A `select` without groupBy is per-root output-shaping (each root its own
    // row) when it projects root fields or collects. Metrics alone roll up
    // globally; a bare rollup (e.g. timeBucket with no select/groupBy) is a
    // single bucket.
    let per_root =
        req.group_by.is_empty() && (!req.plain_fields.is_empty() || !req.collects.is_empty());
    // Each matched root is its own row, so rows are bounded by the paging
    // and the memory budget, not by the group cap; without an `orderBy` only
    // the requested page is reconstructed.
    let per_row = pure_projection || per_root;
    let candidates = if per_row && req.order_by.is_empty() {
        let page_end = req.skip.saturating_add(req.limit.unwrap_or(u64::MAX));
        candidates
            .into_iter()
            .take(usize::try_from(page_end).unwrap_or(usize::MAX))
            .collect()
    } else {
        candidates
    };

    let mut buckets: Vec<GroupBucket> = Vec::new();
    // Family definitions are frozen within one pinned snapshot: a session
    // caches them per candidate (bounded, semantically invisible — P20).
    let mut retained_bytes = candidate_bytes;
    for ord in candidates {
        let snap = budget.map_or_else(
            || Arc::clone(&snap),
            |limit| snap.with_materialization_limit(limit.saturating_sub(retained_bytes) / 16),
        );
        let reader = aggregate_budget::RecordReader::new(
            &snap,
            budget.map(|limit| limit.saturating_sub(retained_bytes)),
        );
        let mut session = reconstruct::ReconstructSession::new(&reader, lmpg.tenant);
        if is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        let Some(json) = session.reconstruct_record(ord)? else {
            continue;
        };
        let row_bytes = aggregate_budget::value_bytes(&json);
        let metric_count = req
            .counts
            .len()
            .saturating_add(req.sums.len())
            .saturating_add(req.mins.len())
            .saturating_add(req.maxs.len())
            .saturating_add(req.expr_avgs.len())
            .saturating_add(req.expr_sums.len())
            .saturating_add(req.avg_terms.len())
            .saturating_add(req.group_by.len())
            .saturating_add(req.plain_fields.len());
        // Reserve space for row clones and per-row metric/key additions before retaining them.
        let row_allowance = row_bytes.saturating_mul(metric_count.saturating_add(4));
        aggregate_budget::check(budget, retained_bytes.saturating_add(row_allowance))?;
        let alias_budget = budget.map(|limit| {
            limit.saturating_sub(retained_bytes.saturating_add(row_allowance))
                / metric_count.saturating_add(1)
        });
        let obj = json.as_object().cloned().unwrap_or_default();

        let mut bucket_keys: Map<String, Json> = Map::new();
        if pure_projection || per_root {
            // A `select` without groupBy is row projection / per-root
            // aggregation, not a global rollup: the hidden root ordinal keeps
            // each root a distinct row; aggregate_to_rows removes it.
            bucket_keys.insert("__rushdb_projection_root".into(), Json::Number(ord.into()));
        }
        if pure_projection {
            for (output, field) in &req.plain_fields {
                if let Some(value) = resolve_field(&obj, field) {
                    bucket_keys.insert(output.clone(), value);
                }
            }
        } else if per_root {
            // Per-root rows with metrics still carry plain field projections.
            for (output, field) in &req.plain_fields {
                if let Some(value) = resolve_field(&obj, field) {
                    bucket_keys.insert(output.clone(), value);
                }
            }

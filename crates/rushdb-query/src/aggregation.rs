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
        }
        for g in &req.group_by {
            // a group-by entry that is a timeBucket alias is resolved by the
            // timeBucket step below; don't leak a separate missing key
            if req.time_bucket_alias.as_deref() == Some(g.as_str()) {
                continue;
            }
            // The key keeps the caller's spelling; lookups by output name
            // compare normalized names.
            let v = resolve_field(&obj, g).unwrap_or(Json::String(MISSING_SENTINEL.into()));
            bucket_keys.insert(g.clone(), v);
        }
        if let Some(tb) = &req.time_bucket {
            let keyed = resolve_field(&obj, &tb.field)
                .and_then(|v| v.as_f64())
                .map_or_else(
                    || Json::String(MISSING_SENTINEL.into()),
                    |ms| Json::Number((tb.floor_millis(ms) as u64).into()),
                );
            // output naming honors the user's `as` alias; the default key is
            // the documented `__bucket_<field>/<unit>` form
            let tb_key = req.time_bucket_alias.clone().unwrap_or_else(|| {
                format!("__bucket_{}/{}", normalize(&tb.field), tb.unit.unit_str())
            });
            bucket_keys.insert(tb_key, keyed);
        }

        if !per_row
            && buckets.len() >= MAX_AGGREGATE_BUCKETS
            && !buckets.iter().any(|b| b.keys == bucket_keys)
        {
            return Err(QueryError::ResourceLimit(format!("aggregate exceeds {MAX_AGGREGATE_BUCKETS} distinct groups; narrow the groupBy or add filters")));
        }
        // Per-row keys carry the root ordinal, so they never repeat.
        let previous_bucket_bytes = if per_row {
            0
        } else {
            buckets
                .iter()
                .find(|b| b.keys == bucket_keys)
                .map_or(0, aggregate_budget::bucket_bytes)
        };
        let bucket = if per_row {
            new_bucket(&mut buckets, bucket_keys)
        } else {
            ensure_bucket(&mut buckets, bucket_keys)
        };

        // $count '*' = distinct root identity; each row already unique.
        bucket.count_roots += 1;

        for (out, field) in &req.counts {
            if req.hidden_outputs.contains(out) {
                continue;
            }
            let target_key = out.clone();
            if field.as_str() == "*" {
                continue;
            }
            // P20 fix: a `$count` over a related-record alias (`$count: "$battle"`)
            // counts the distinct related records, not a root-record field. The
            // alias comes from the `where`/`related` traversal specs.
            if let Some(related) = count_related(
                lmpg,
                &snap,
                lmpg.tenant,
                ord,
                &alias_specs,
                field,
                alias_budget,
            )? {
                aggregate_budget::check(alias_budget, related.len().saturating_mul(64))?;
                for _other in related {
                    bump_count(bucket, &target_key);
                }
                continue;
            }
            if resolve_field(&obj, field).is_some() {
                bump_count(bucket, &target_key);
            }
        }
        for (out, field) in &req.sums {
            // alias metrics: `$sum: "$employee.salary"` aggregates over the
            // root's related records via the `where` traversal alias.
            if let Some(values) = alias_field_values(
                lmpg,
                &snap,
                lmpg.tenant,
                ord,
                &alias_specs,
                field,
                alias_budget,
            )? {
                let key = out.clone();
                let cur = bucket
                    .sums
                    .get(&key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let sum = values
                    .iter()
                    .try_fold(cur, |sum, value| -> Result<f64, QueryError> {
                        if !value.is_number() {
                            return Ok(sum);
                        }
                        let value = exact_f64(value).ok_or_else(|| {
                            QueryError::InvalidPredicate(
                                "aggregate integer is not exactly representable as f64".into(),
                            )
                        })?;
                        Ok(sum + value)
                    })?;
                bucket.sums.insert(key, serde_json::json!(sum));
            } else if let Some(Json::Number(n)) = resolve_field_numeric(&obj, field) {
                let key = out.clone();
                let cur = bucket
                    .sums
                    .get(&key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let next = cur
                    + exact_f64(&Json::Number(n.clone())).ok_or_else(|| {
                        QueryError::InvalidPredicate(
                            "aggregate integer is not exactly representable as f64".into(),
                        )
                    })?;
                bucket.sums.insert(key, serde_json::json!(next));
            }
        }
        for (out, expr) in &req.expr_sums {
            if let Some(v) = eval_row_expr(expr, &obj) {
                let key = out.clone();
                let cur = bucket
                    .sums
                    .get(&key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                bucket.sums.insert(key, serde_json::json!(cur + v));
            }
        }
        for (out, expr) in &req.expr_avgs {
            if let Some(v) = eval_row_expr(expr, &obj) {
                let key = out.clone();
                let cur = bucket
                    .sums
                    .get(&key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                bucket.sums.insert(key, serde_json::json!(cur + v));
                bump_avg_count(bucket, format!("__avg_n_{out}"), 1);
            }
        }
        for (out, field) in &req.avg_terms {
            if let Some(values) = alias_field_values(
                lmpg,
                &snap,
                lmpg.tenant,
                ord,
                &alias_specs,
                field,
                alias_budget,
            )? {
                let key = out.clone();
                let sum = values
                    .iter()
                    .try_fold(0.0, |sum, value| -> Result<f64, QueryError> {
                        if !value.is_number() {
                            return Ok(sum);
                        }
                        let value = exact_f64(value).ok_or_else(|| {
                            QueryError::InvalidPredicate(
                                "aggregate integer is not exactly representable as f64".into(),
                            )
                        })?;
                        Ok(sum + value)
                    })?;
                let cur = bucket
                    .sums
                    .get(&key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                bucket.sums.insert(key, serde_json::json!(cur + sum));
                let added = values.iter().filter(|v| v.is_number()).count() as u64;
                bump_avg_count(bucket, format!("__avg_n_{out}"), added);
            } else if let Some(Json::Number(n)) = resolve_field_numeric(&obj, field) {
                let key = out.clone();
                let cur = bucket
                    .sums
                    .get(&key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let value = exact_f64(&Json::Number(n.clone())).ok_or_else(|| {
                    QueryError::InvalidPredicate(
                        "aggregate integer is not exactly representable as f64".into(),
                    )
                })?;
                bucket.sums.insert(key, serde_json::json!(cur + value));
                bump_avg_count(bucket, format!("__avg_n_{out}"), 1);
            }
        }
        for (out, field) in &req.mins {
            if let Some(values) = alias_field_values(
                lmpg,
                &snap,
                lmpg.tenant,
                ord,
                &alias_specs,
                field,
                alias_budget,
            )? {
                for v in values {
                    minmax(bucket, out.clone(), &v, true);
                }
            } else if let Some(v) = resolve_field(&obj, field) {
                minmax(bucket, out.clone(), &v, true);
            }
        }
        for (out, field) in &req.maxs {
            if let Some(values) = alias_field_values(
                lmpg,
                &snap,
                lmpg.tenant,
                ord,
                &alias_specs,
                field,
                alias_budget,
            )? {
                for v in values {
                    minmax(bucket, out.clone(), &v, false);
                }
            } else if let Some(v) = resolve_field(&obj, field) {
                minmax(bucket, out.clone(), &v, false);
            }
        }
        retained_bytes = retained_bytes
            .saturating_sub(previous_bucket_bytes)
            .saturating_add(aggregate_budget::bucket_bytes(bucket));
        aggregate_budget::check(budget, retained_bytes)?;
    }

    // Derived outputs ($ref chains): source sums materialize first, then
    // refs resolve in declaration order; cycles surface as unresolved refs.
    if !req.outputs.is_empty() || !req.avg_terms.is_empty() || !req.expr_avgs.is_empty() {
        let output_bytes = req
            .outputs
            .iter()
            .fold(0usize, |sum, (name, _)| {
                sum.saturating_add(name.len()).saturating_add(256)
            })
            .saturating_mul(buckets.len());
        aggregate_budget::check(
            budget,
            retained_bytes
                .saturating_mul(3)
                .saturating_add(output_bytes),
        )?;
        for b in &mut buckets {
            if is_cancelled() {
                return Err(QueryError::Cancelled);
            }

//! Access-path selection and bounded candidate admission.

use super::{
    access, canonical_id, hex_decode_16, normalize, scalar_equal, Expr, FieldPred, Json, Lmpg,
    LogicalType, QueryError, QuerySnapshot, Scalar, SearchRequest, TypeState,
};
use rushdb_lmpg::reader::TryRead;

/// Index-only candidate plan for a `where` conjunction. Eligible positive
/// leaves (`field == scalar`, `field IN`, `$exists: true`, null, and numeric
/// range) resolve to maintained posting/presence owner sets; residual leaves
/// (`$ne`, contains, `$type`, logical, traversal) are evaluated later on the
/// reduced candidate set. Eligible leaves are materialized in ascending
/// estimated cardinality (most selective first) with an early-out once the
/// intersection is empty. `exact` is true when every leaf was answered AND
/// there are no traversal predicates, so the caller may skip per-candidate
/// evaluation and the exact-count rescan.
pub(super) struct IndexPlan {
    /// Candidate owners, ascending.
    pub(super) ords: OwnerSet,
    pub(super) exact: bool,
    /// Maintained owner count for the query, when known (single unscoped
    /// `$exists`): lets the executor report an exact total without
    /// materializing or counting every candidate.
    pub(super) known_total: Option<u64>,
    /// The conjuncts the owner set does not answer exactly (non-indexable
    /// leaves, and null-only leaves, a superset prefilter), for the executor
    /// to evaluate on each candidate instead of the whole `where`. `None`
    /// when there are none.
    pub(super) residual: Option<Expr>,
}

/// Cap on the smallest eligible leaf the planner walks occurrence by
/// occurrence; beyond it a full scan is cheaper. A leaf a declared ordered
/// index answers is never capped: its entries stream into a compressed
/// owner set (a few bits per owner when dense), far cheaper than scanning
/// and evaluating every record (READ-1: `city = Oslo` at ~2M owners fell
/// back to a 10M-record scan). Unknown estimates sort last.
const PLANNER_MATERIALIZE_CAP: u64 = 2_000_000;

/// An unindexed predicate leaf that walks more owners than this is counted
/// in [`unindexed_walks_total`]: the signal that a declared ordered index
/// would pay off (DC2 keeps no automatic value postings).
pub const UNINDEXED_WALK_THRESHOLD: u64 = 10_000;

static UNINDEXED_WALKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Unindexed leaf walks over more than [`UNINDEXED_WALK_THRESHOLD`] owners
/// in this process (`rushdb_unindexed_walks_total`).
#[must_use]
pub fn unindexed_walks_total() -> u64 {
    UNINDEXED_WALKS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Largest page the bounded presence merge will materialize for a known-total
/// count plan; beyond it the normal path (full materialization) is used.
const PLANNER_BOUNDED_PAGE_CAP: usize = 100_000;

#[expect(
    clippy::too_many_lines,
    reason = "the planner is a single cohesive access-path selection routine; splitting it would obscure the fall-through ordering"
)]
pub(super) fn index_candidates_with_budget(
    lmpg: &Lmpg,
    snap: &QuerySnapshot,
    req: &SearchRequest,
    budget: Option<usize>,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<Option<IndexPlan>, QueryError> {
    let owner_limit = budget.map_or(usize::MAX, |bytes| bytes / OWNER_SET_BYTES_PER_OWNER);
    if req.force_scan {
        return Ok(None);
    }
    let Some(expr) = &req.where_ else {
        return Ok(None);
    };
    let mut leaves: Vec<&Expr> = Vec::new();
    collect_leaves(expr, &mut leaves);
    if leaves.is_empty() {
        return Ok(None);
    }
    let mut eligible: Vec<(u64, &Expr)> = Vec::new();
    let mut residual: Vec<Expr> = Vec::new();
    let mut has_residual = false;
    let mut exactable = true;
    for leaf in leaves {
        if is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        if let Some(estimate) = leaf_estimate(snap, lmpg.tenant, leaf, &req.labels)? {
            if leaf_is_null_only(leaf) {
                residual.push(leaf.clone());
            }
            eligible.push((estimate, leaf));
        } else {
            residual.push(leaf.clone());
            has_residual = true;
            if leaf_is_null_only(leaf) {
                exactable = false;
            }
        }
    }
    if eligible.is_empty() {
        return Ok(None);
    }
    // Single unscoped `$exists: true` with a maintained owner count: answer the
    // total from the counter and materialize only the requested page.
    if !has_residual
        && exactable
        && req.related.is_empty()
        && req.order_by.is_empty()
        && eligible.len() == 1
    {
        if let Expr::Field(FieldPred::Exists(name, true)) = eligible[0].1 {
            if let Some(limit) = req.pagination.limit {
                let page = req
                    .pagination
                    .skip
                    .saturating_add(limit)
                    .min(u64::try_from(PLANNER_BOUNDED_PAGE_CAP).unwrap_or(u64::MAX));
                let page = usize::try_from(page).unwrap_or(PLANNER_BOUNDED_PAGE_CAP);
                if let Some(total) = total_support(snap, lmpg.tenant, name, &req.labels)? {
                    let ords =
                        owners_with_presence_bounded(snap, lmpg.tenant, name, &req.labels, page)?;
                    return Ok(Some(IndexPlan {
                        ords: ords.into_iter().collect(),
                        exact: true,
                        known_total: Some(total),
                        residual: None,
                    }));
                }
            }
        }
    }
    eligible.sort_by_key(|(estimate, _)| *estimate);
    if eligible[0].0 != u64::MAX
        && eligible[0].0 > PLANNER_MATERIALIZE_CAP
        && !crate::index_access::answers(snap, lmpg.tenant, eligible[0].1, &req.labels)?
    {
        // The most selective eligible leaf is still too large to materialize.
        // (Unknown estimates are `u64::MAX` and are materialized normally.)
        return Ok(None);
    }
    // Every leaf but `$id` resolves under the requested labels (index
    // entries, presence and value rows are kept per label), so one such leaf
    // in the conjunction scopes the whole intersection.
    let label_scoped = req.labels.is_empty()
        || eligible
            .iter()
            .any(|(_, leaf)| !is_id_leaf(leaf) && !matches!(leaf, Expr::Or(_)));
    let mut acc: Option<OwnerSet> = None;
    for (_, leaf) in eligible {
        if is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        if acc.as_ref().is_some_and(OwnerSet::is_empty) {
            break;
        }
        // Later leaves are checked on the owners earlier leaves left
        // (`LeafScope::candidates`), not walked over their population.
        let scope = LeafScope {
            labels: &req.labels,
            owner_limit,
            candidates: acc.as_ref(),
        };
        let Some(owners) = leaf_owners(snap, lmpg.tenant, leaf, &scope)? else {
            // Classified eligible but not materializable: fall back safely.
            return Ok(None);
        };
        acc = Some(match acc {
            None => owners,
            Some(prev) => prev & owners,
        });
    }
    let mut out = acc.unwrap_or_default();
    if !label_scoped {
        // Only `$id` leaves: probe labels for that (small) id set.
        let mut outside = Vec::new();
        for ord in &out {
            if is_cancelled() {
                return Err(QueryError::Cancelled);
            }
            let mut allowed = false;
            for label in &req.labels {
                let key = rushdb_types::keys::label_member_key(lmpg.tenant, label, ord);
                if snap.get(&key)?.is_some() {
                    allowed = true;
                    break;
                }
            }
            if !allowed {
                outside.push(ord);
            }
        }
        for ord in outside {
            out.remove(ord);
        }
    }
    let exact = !has_residual && exactable && req.related.is_empty();
    Ok(Some(IndexPlan {
        ords: out,
        exact,
        known_total: None,
        residual: match residual.len() {
            0 => None,
            1 => residual.pop(),
            _ => Some(Expr::And(residual)),
        },
    }))
}

/// Flatten a top-level `And` into conjuncts; any other node is a single
/// conjunct (eligible or residual), so partial-index planning can still use
/// the other conjuncts.
pub(super) fn collect_leaves<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::And(items) => {
            for item in items {
                collect_leaves(item, out);
            }
        }
        _ => out.push(expr),
    }
}

/// Null-only leaves are a superset prefilter (a record may hold both a null
/// and a non-null occurrence of one property), so they never take the exact
/// shortcut.
fn leaf_is_null_only(expr: &Expr) -> bool {
    matches!(expr, Expr::Field(FieldPred::Eq(_, value)) if value.is_null())
        || matches!(expr, Expr::Field(FieldPred::IsNull(_, true)))
}

/// Cardinality estimate for an indexable leaf, restricted to `labels` when
/// given. `None` = residual (not served by presence/dictionary lookups).
/// `u64::MAX` = eligible but unknown (sorts last).
fn leaf_estimate<R: TryRead>(
    snap: &R,
    tenant: rushdb_types::ids::TenantId,
    expr: &Expr,
    labels: &[String],
) -> Result<Option<u64>, QueryError> {
    // A prefix leaf is answered only by a declared ordered index.
    if matches!(expr, Expr::Field(FieldPred::StartsWith(..))) {
        return Ok(crate::index_access::answers(snap, tenant, expr, labels)?.then_some(u64::MAX));
    }
    Ok(match expr {
        Expr::Field(FieldPred::Eq(_, value)) if value.is_null() => Some(u64::MAX),
        Expr::Field(FieldPred::Eq(name, value)) => {
            estimate_values(snap, tenant, name, std::slice::from_ref(value), labels)?
        }
        Expr::Field(FieldPred::In(name, values)) if values.iter().all(|v| !v.is_null()) => {
            estimate_values(snap, tenant, name, values, labels)?
        }
        // `$id` resolves through the record-id index: one lookup, one ordinal.
        Expr::Field(FieldPred::Id(_)) => Some(1),

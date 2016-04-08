//! Declared ordered indexes in the planner (SD-4.4): when every
//! requested label has a `Ready` ordered index on a leaf's property, the leaf
//! is answered from `INDEX_ENTRY` rows instead of a presence-narrowed walk.
//! Each candidate entry is re-checked against the predicate, so the owner
//! set is exact.

use rushdb_lmpg::index;
use rushdb_lmpg::reader::TryRead;
use rushdb_types::encoding::{decode_order_preserving, encode_order_preserving, logical_tag};
use rushdb_types::ids::TenantId;
use rushdb_types::index::IndexDef;
use rushdb_types::model::LogicalType;
use rushdb_types::value::{normalize_name, Scalar};

use crate::planner::{extend_owners, range_scalar_matches, scalar_variants, OwnerSet, RangeOp};
use crate::{Expr, FieldPred, QueryError};

type Json = serde_json::Value;

/// The `Ready` ordered indexes covering property `name` for every label in
/// `labels` (one per label), or `None` when some label has none or no label
/// is given (an index belongs to a label).
pub(crate) fn covering<R: TryRead>(
    snap: &R,
    tenant: TenantId,
    name: &str,
    labels: &[String],
) -> Result<Option<Vec<IndexDef>>, QueryError> {
    let norm = normalize_name(name);
    if labels.is_empty() || norm.is_empty() || rushdb_types::value::is_reserved_name(&norm) {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(labels.len());
    for label in labels {
        match index::ready_ordered(snap, tenant, label, &norm)? {
            Some(def) => out.push(def),
            None => return Ok(None),
        }
    }
    Ok(Some(out))
}

/// `[start, end)` covering every entry that begins with `prefix`.
fn prefix_bounds(prefix: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    let end = rushdb_storage::prefix_range(&prefix).1;
    (prefix, end)
}

fn scan<R: TryRead>(
    snap: &R,
    tenant: TenantId,
    defs: &[IndexDef],
    (start, end): (&[u8], &[u8]),
    keep: &dyn Fn(&[u8]) -> bool,
    owners: &mut OwnerSet,
    owner_limit: usize,
) -> Result<(), QueryError> {
    // Streamed straight into the owner set: an entry range of millions of
    // owners never exists as a row list.
    let mut over_budget = None;
    for def in defs {
        index::for_each_entry(snap, tenant, def.id, start, end, &mut |entry, owner| {
            if !keep(entry) {
                return true;
            }
            match extend_owners(owners, std::iter::once(owner), owner_limit) {
                Ok(()) => true,
                Err(error) => {
                    over_budget = Some(error);
                    false
                }
            }
        })?;
        if let Some(error) = over_budget {
            return Err(error);
        }
    }
    Ok(())
}

fn entry_of(ty: LogicalType, scalar: &Scalar) -> Option<Vec<u8>> {
    let mut entry = vec![logical_tag(ty)];
    entry.extend(encode_order_preserving(ty, scalar).ok()?);
    Some(entry)
}

/// Owners whose value equals any of `values` (every type variant a value
/// may be stored under, as the presence path probes).
fn equal<R: TryRead>(
    snap: &R,
    tenant: TenantId,
    defs: &[IndexDef],
    values: &[Json],
    owner_limit: usize,
) -> Result<Option<OwnerSet>, QueryError> {
    let mut owners = OwnerSet::new();
    for value in values {
        let Some(variants) = scalar_variants(value) else {
            return Ok(None);
        };
        for (ty, scalar) in variants {
            let Some(entry) = entry_of(ty, &scalar) else {
                return Ok(None);
            };
            let (start, end) = prefix_bounds(entry.clone());
            scan(
                snap,
                tenant,
                defs,
                (&start, &end),
                &|found| found == entry.as_slice(),
                &mut owners,
                owner_limit,
            )?;
        }
    }
    Ok(Some(owners))
}

/// Owners holding a numeric value satisfying `op bound`.
fn range<R: TryRead>(
    snap: &R,
    tenant: TenantId,
    defs: &[IndexDef],
    op: RangeOp,
    bound: f64,
    owner_limit: usize,
) -> Result<Option<OwnerSet>, QueryError> {
    let mut owners = OwnerSet::new();
    let above = matches!(op, RangeOp::Gt | RangeOp::Gte);
    for ty in [
        LogicalType::Int64,
        LogicalType::Float64,
        LogicalType::DateTime,
    ] {
        let tag = logical_tag(ty);
        // Bound the scan at the bound's encoding, rounded outward for the
        // integer domains; the exact comparison happens on each entry.
        let edge = match ty {
            LogicalType::Float64 => Scalar::Float64(bound),
            LogicalType::Int64 => Scalar::Int64(outward(bound, above)),
            _ => Scalar::DateTime(outward(bound, above)),
        };
        let Some(edge) = entry_of(ty, &edge) else {
            continue;
        };
        let (start, end) = if above {
            (edge, vec![tag + 1])
        } else {
            (vec![tag], rushdb_storage::prefix_range(&edge).1)
        };
        scan(
            snap,
            tenant,
            defs,
            (&start, &end),

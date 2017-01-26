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

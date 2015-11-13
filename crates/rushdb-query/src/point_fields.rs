//! Record properties read by point reads instead of rebuilding the record
//!: the ordering properties of an ordered page, the properties a
//! residual predicate names, and the columns a SQL label scan reads.

use crate::planner::{family_id, pids_of};
use crate::{access, normalize, reconstruct, Expr, FieldPred, Json, QueryError, QuerySnapshot};
use rushdb_lmpg::reader::TryRead;
use serde_json::Map;

/// Record properties read with one point read per typed definition instead
/// of rebuilding the record (READ-1: sorting 667K matches, or checking an
/// unindexed `age < 25` on them, rebuilt every one). Covers scalar record
/// properties only; [`Self::resolve`] answers `None` for a system field or a
/// property stored as an array anywhere, and the caller rebuilds the record.
pub struct PointFields(Vec<(String, Vec<u64>)>);

impl PointFields {
    /// Point reads for `names`, each row keyed by the name as given; `None`
    /// when one of them needs the whole record.
    ///
    /// # Errors
    /// Returns storage read errors from the property catalog.
    pub fn resolve<'n>(
        snap: &QuerySnapshot,
        tenant: rushdb_types::ids::TenantId,
        names: impl IntoIterator<Item = &'n str>,
    ) -> Result<Option<Self>, QueryError> {
        let mut fields = Vec::new();
        for name in names {
            let Some(pids) = scalar_pids(snap, tenant, name)? else {
                return Ok(None);
            };
            fields.push((name.to_owned(), pids));
        }
        Ok(Some(Self(fields)))
    }

    /// The properties `predicate` reads, or `None` when it needs the whole
    /// record.
    pub(super) fn for_predicate(
        snap: &QuerySnapshot,
        tenant: rushdb_types::ids::TenantId,
        predicate: &Expr,
    ) -> Result<Option<Self>, QueryError> {
        let mut names = std::collections::BTreeSet::new();
        if !predicate_fields(predicate, &mut names) {
            return Ok(None);
        }
        Self::resolve(snap, tenant, names.iter().map(String::as_str))
    }

    /// The fields of record `ord`: each one's value, as the rebuilt record
    /// would carry it (the highest typed definition holding
    /// one wins, as when occurrences fold into a record).
    ///
    /// # Errors
    /// Returns storage read errors and corrupt-occurrence errors.
    pub fn row(
        &self,
        snap: &QuerySnapshot,
        tenant: rushdb_types::ids::TenantId,
        ord: u64,
    ) -> Result<Map<String, Json>, QueryError> {
        let mut row = Map::new();
        for (field, pids) in &self.0 {
            // Highest first: the first definition holding a value wins.
            for &pid in pids.iter().rev() {
                if let Some(occ) = access::occurrence(
                    snap,
                    tenant,
                    rushdb_types::ids::OwnerKind::Record,
                    ord,
                    pid,
                    0,
                )? {
                    row.insert(
                        field.clone(),
                        reconstruct::decode_value(occ.value, &occ.payload)?,
                    );
                    break;
                }
            }
        }
        Ok(row)
    }
}

/// Collect the property names `expr` reads into `out`; `false` when it needs
/// the whole record (`$id`, traversals).
fn predicate_fields(expr: &Expr, out: &mut std::collections::BTreeSet<String>) -> bool {
    match expr {
        Expr::And(items) | Expr::Or(items) | Expr::Nor(items) => {
            items.iter().all(|item| predicate_fields(item, out))
        }
        Expr::Xor(a, b) => predicate_fields(a, out) && predicate_fields(b, out),
        Expr::Not(inner) => predicate_fields(inner, out),
        Expr::Related(_) | Expr::Field(FieldPred::Id(_)) => false,
        Expr::Field(
            FieldPred::Eq(name, _)
            | FieldPred::Ne(name, _)

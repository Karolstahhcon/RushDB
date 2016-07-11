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

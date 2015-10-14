//! L03: superseded values leave no residue. Values live inline in one
//! occurrence row per value, and the per-property value dictionary
//! is derived at apply time, so an update or delete must not leave an
//! old occurrence row or a stale dictionary entry behind.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::{OwnerKind, TenantId};
use serde_json::json;

fn occurrence_rows(eng: &Lmpg) -> usize {
    let prefix = rushdb_types::keys::occ_kind_prefix(eng.tenant, OwnerKind::Record);
    eng.store.try_range_prefix(&prefix).unwrap().len()
}

fn dictionary_entries(eng: &Lmpg) -> usize {

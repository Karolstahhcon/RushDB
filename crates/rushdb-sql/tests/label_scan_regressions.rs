//! Regressions for the lazy SQL label scan.
//!
//! The scan range is a byte prefix, so it must re-validate each row against the
//! label's exact membership key: a label whose name prefixes another (`user`
//! vs `user_admin`) must not leak rows, and the ordinal suffix must be read at
//! its fixed offset (an ordinal byte equal to `/` must not be mistaken for the
//! separator).

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_sql::execute_sql;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn graph(tenant: u64) -> Lmpg {
    Lmpg::new(
        Arc::new(Store::new_in_memory()),
        TenantId::from_ordinal(tenant),
    )
}


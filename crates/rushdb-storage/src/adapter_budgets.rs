//! Per-adapter resource-budget attribution: a named child of the
//! shared cache/phase budget per adapter (vector, SQL, graph), so each
//! adapter's `peak`/`used` (a real, measured high-water mark — see
//! [`crate::resources::ByteBudget::snapshot`]) is individually observable
//! instead of only the two pooled totals. Each child's limit equals its
//! parent's, so this is pure attribution: creating it does not narrow the
//! shared capacity the adapters already contend on (see
//! [`crate::resources::ByteBudget::child`]).

use crate::resources::ByteBudget;
use crate::{StorageError, Store};

/// One named attribution child per adapter. `None` exactly when its parent
/// budget (`logical_cache_resources` / `phase_resources`) is `None`.
pub(crate) struct AdapterBudgets {
    vector_cache: Option<ByteBudget>,
    sql_phase: Option<ByteBudget>,
    graph_phase: Option<ByteBudget>,
}

impl AdapterBudgets {
    pub(crate) fn new(
        cache: Option<&ByteBudget>,

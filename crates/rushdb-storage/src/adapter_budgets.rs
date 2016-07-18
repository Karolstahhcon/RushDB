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

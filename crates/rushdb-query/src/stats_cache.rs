//! Planner statistics on demand: a value distribution is read
//! from the store's evictable [`rushdb_storage::PlannerStats`] cache, or
//! loaded by a budgeted walk of the value dictionary on a miss. Estimates
//! only; exact answers read counters at the snapshot.

use std::sync::Arc;

use rushdb_lmpg::derived::{value_distribution, WalkBudget};
use rushdb_lmpg::reader::TryRead;
use rushdb_storage::planner_stats::PlannerStatsEntry;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::stats::Histogram;

use crate::QueryError;


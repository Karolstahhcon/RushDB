//! Index candidates for an executor that checks every predicate itself.
//!
//! The SQL label scan filters rows with `DataFusion`, but it reads
//! candidates the way records reads do: from the index plan when an index
//! answers a condition, from a label walk otherwise, and it is refused the
//! same way when a filter without a usable index would check more records
//! than the unindexed-walk limit allows (QRY-3: none by default).

use rushdb_lmpg::Lmpg;

use crate::planner::OwnerSet;
use crate::{
    check_walk_budget, index_candidates_with_budget, working_set_budget, Expr, QueryError,
    QuerySnapshot, SearchRequest,
};

/// The index plan's candidate ordinals, ascending. A superset of the
/// records matching the conditions it was planned for, or exactly them.
pub struct PlannedCandidates {
    ords: roaring::treemap::IntoIter,
    exact: bool,
}

impl PlannedCandidates {
    /// Every candidate is a live record matching every condition: none
    /// needs checking.
    #[must_use]
    pub fn exact(&self) -> bool {
        self.exact
    }
}

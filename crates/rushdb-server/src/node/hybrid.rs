//! Hybrid search: one predicate-resolved candidate set ranked by a vector
//! leg, a text leg or both, fused by reciprocal rank.

use super::{access, append_bounded_json_response, Arc, HashSet, OwnerKind, RequestScope};

impl RequestScope<'_> {
    /// Execute [`Self::hybrid_search_in_view`] against a caller-owned
    /// execution context's pinned view and remaining response-byte budget.
    ///
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub(crate) fn hybrid_search_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        query: &rushdb_protocol::HybridQuery,
    ) -> Result<serde_json::Value, String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let result = self.hybrid_search_in_view(query, context.read_view())?;
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        Ok(result)
    }

    /// Combined graph + vector + lexical query (document 26's hybrid
    /// pattern), against a caller-owned pinned view and work budget.
    ///
    /// The `labels`/`predicate` graph filter is resolved exactly once, via
    /// the same cardinality-based access-path planner `find_records` uses
    /// (`matching_ordinals_with_budget`), into one candidate owner-ordinal
    /// set. Both legs search only inside that set — candidate resolution
    /// happens before ranking for both the ANN graph and the lexical index,
    /// never as a post-filter that could leak a hidden owner into a fused
    /// top-k slot. The two independently correct rankings are then combined
    /// by deterministic reciprocal-rank fusion; raw scores never cross
    /// providers.
    ///
    /// Every fused hit is reconstructed through an
    /// [`rushdb_lmpg::AuthorizedView`] before being emitted — the same
    /// policy gate [`Self::search_vectors_in_view`] applies, and a stronger
    /// one than the bare lexical path had, so this is not added to
    /// `protocol.rs`'s `PolicyEnforced` fail-closed list.
    ///
    /// # Errors
    ///
    /// Returns an error message when the operation fails, including when
    /// neither leg is set, a named vector index/text profile is unknown, or
    /// `PolicyEnforced` denies every requested label.
    #[expect(
        clippy::too_many_lines,
        reason = "one cohesive candidate-resolution -> dual-leg search -> fusion -> \
                  reconstruction pipeline, the same shape as search_vectors_in_view; \
                  splitting it would scatter the shared candidate/budget borrows"
    )]
    pub(super) fn hybrid_search_in_view(
        &self,
        query: &rushdb_protocol::HybridQuery,
        snap: &Arc<rushdb_storage::ReadView>,
    ) -> Result<serde_json::Value, String> {
        if query.vector.is_none() && query.text.is_none() {
            return Err("hybrid query requires at least one of `vector`/`text`".to_string());
        }
        validate(query)?;
        if let Some(active) = rushdb_authz::load_active_policy_in_view(snap, self.tenant())
            .map_err(|error| error.to_string())?
        {
            if active.mode == rushdb_authz::AuthorizationMode::PolicyEnforced
                && !(if query.labels.is_empty() {
                    active.policy.allows_every_label()
                } else {
                    query
                        .labels
                        .iter()
                        .all(|label| active.policy.allows_label(label))
                })
            {
                return Err("hybrid query denied by policy".into());
            }
        }
        let limit = usize::try_from(query.limit).unwrap_or(usize::MAX).max(1);

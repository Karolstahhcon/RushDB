//! Which path answers a vector search. `auto` (the default) searches
//! exactly while the index is small enough that scoring every vector is
//! cheap, and on the ANN graph above `resources.vector-exact-max-pool`; a
//! caller can force either path. The response reports the path taken.

use std::sync::Arc;

use rushdb_protocol::VectorSearchMode;
use rushdb_types::occurrence::Similarity;
use rushdb_vector::search::{FallbackReason, SearchMode, SearchPath};
use rushdb_vector::{EmbeddingSpace, ScoredSource, SourceRef, VectorEngine};

use super::Node;

/// One vector search's inputs, shared by `query.vector` and the vector leg
/// of `query.hybrid`.
pub(super) struct VectorSearch<'a> {
    pub query: &'a [f32],
    /// `offset + limit`: how many ranked hits the page needs.
    pub k: usize,
    pub mode: Option<VectorSearchMode>,
    pub ef: Option<usize>,
    pub min_score: Option<f32>,
}

impl Node {
    /// Run one vector search over `space` and report the path taken. `auto`
    /// answers exactly up to `vector-exact-max-pool` vectors; above it, on
    /// the ANN graph once resident, and exactly while a background build
    /// makes it resident (a query never waits for a build).
    ///
    /// # Errors
    ///
    /// A forced `ann` on an index whose metric has no ANN graph, or a
    /// failed search.
    pub(super) fn run_vector_search(
        &self,
        vectors: &Arc<VectorEngine>,
        snap: &rushdb_storage::ReadView,
        space: &EmbeddingSpace,
        search: &VectorSearch<'_>,
        candidate: Option<&dyn Fn(SourceRef) -> bool>,
    ) -> Result<(Vec<ScoredSource>, serde_json::Value), String> {
        let pool = vectors
            .ready_vectors_in_view(snap, space)
            .map_err(|e| e.to_string())?;
        let (mut mode, auto) = self.vector_search_mode(search, space.similarity, pool)?;
        let mut warming = false;
        if auto
            && matches!(mode, SearchMode::Ann { .. })
            && !vectors
                .ann_resident_in_view(snap, space)
                .map_err(|e| e.to_string())?
        {
            vectors.warm_ann_in_background(snap, space);
            mode = SearchMode::Exact;
            warming = true;
        }
        let outcome = vectors
            .search_in_view(
                snap,
                space,
                search.query,
                search.k,
                mode,
                candidate,
                search.min_score,
            )
            .map_err(|e| e.to_string())?;
        let mut report = vector_search_report(mode, outcome.path, pool);
        if warming {

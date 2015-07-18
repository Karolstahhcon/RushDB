//! One vector search: exact or approximate (ANN), an optional minimum
//! score, and the path that actually answered.

use std::sync::Arc;

use rushdb_storage::{ReadView, StorageError};

use crate::ann::space_version;
use crate::{EmbeddingSpace, ScoredSource, SourceRef, VectorEngine, VectorError};

/// A space's background ANN build.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Warmup {
    /// A build is running.
    Building,
    /// The graph of the space at `version` did not fit the vector cache
    /// budget; no build is started again until the space changes.
    NotAdmitted { version: u64 },
}

/// How a search ranks its candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    /// Score every candidate.
    Exact,
    /// Gather candidates from the ANN graph and rerank them exactly.
    Ann {
        /// Gather breadth; at least `k` is gathered.
        ef: usize,
    },
}

/// Why an ANN search was answered by exact search instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackReason {
    /// The space's metric has no ANN graph (only cosine does).
    Metric,
    /// The ANN graph does not fit the vector cache budget.
    Budget,
    /// The gather found too few candidates passing the filter.
    LowConfidence,
}

/// The path that answered a search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchPath {
    /// Exact search, as requested.
    Exact,
    /// ANN gather with exact rerank.
    Ann,
    /// Exact search after an ANN request could not be answered by ANN.
    ExactFallback(FallbackReason),
}

/// Ranked hits, best first, and the path that produced them.
#[derive(Debug, Clone)]
pub struct SearchOutcome {
    /// Ranked hits, best first.
    pub hits: Vec<ScoredSource>,
    /// The path that produced them.
    pub path: SearchPath,
}

impl VectorEngine {

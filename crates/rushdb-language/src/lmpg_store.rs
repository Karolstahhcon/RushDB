//! Read-only Grafeo storage contract backed directly by one LMPG tenant.
//!
//! Records are reconstructed from their occurrences on demand, with the
//! property families cached for the whole statement. Equality lookups go
//! through declared ordered indexes when every label holding the property
//! has one (`has_property_index`); otherwise Grafeo scans the label.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use grafeo_common::types::{EdgeId, EpochId, NodeId, PropertyKey, TransactionId, Value};
use grafeo_common::utils::hash::FxHashMap;
use grafeo_core::graph::lpg::{CompareOp, Edge, Node};
use grafeo_core::graph::{Direction, GraphStore, GraphStoreSearch};
use grafeo_core::statistics::Statistics;
use rushdb_lmpg::reader::try_access as access;
use rushdb_lmpg::reconstruct::{reconstruct_relationship, FamilyCache, ReconstructSession};
use rushdb_storage::{OverlayView, StorageError};
use rushdb_types::ids::TenantId;
use rushdb_types::index::IndexDef;
use rushdb_types::keys::{key, P_REL_BY_ORD};
use rushdb_types::model::LogicalType;
use rushdb_types::value::{scalar_cmp, Scalar};
use serde_json::Value as Json;

use crate::json_to_value_checked;

/// Read-only Grafeo storage contract backed directly by one LMPG tenant.
/// Every match of one text search, by record ordinal, keyed by (index,
/// query).
type TextMatches = HashMap<(String, String), Arc<HashMap<u64, f64>>>;

pub(crate) struct LmpgGraphStore {
    view: Arc<OverlayView>,
    tenant: TenantId,
    failure: Mutex<Option<StorageError>>,
    /// Families resolved so far by this statement's reconstructions.
    families: Mutex<FamilyCache>,
    /// Per property: the ordered indexes answering equality on it (one per
    /// label holding it), or `None` when some label has none.
    equality_indexes: Mutex<HashMap<String, Option<Vec<IndexDef>>>>,
    /// The node's text indexes; `text_match`/`text_score` search them.
    text: Option<Arc<rushdb_textsearch::TextIndexes>>,
    /// Every match of one (index, query), by record ordinal: per-row
    /// scoring reads it instead of searching again.
    text_matches: Mutex<TextMatches>,
    /// A failed text search (unknown option, unparsable query), reported
    /// like a storage failure at the statement boundary.
    text_failure: Mutex<Option<String>>,
}

impl LmpgGraphStore {
    /// `(records, relationships)` from the label and relationship catalogs.
    fn catalog_totals(&self) -> (u64, u64) {
        let nodes = self
            .read(|| access::label_counts(&self.view, self.tenant))
            .iter()
            .map(|(_, count)| count)
            .sum();
        let edges = self
            .read(|| access::relationship_catalog(&self.view, self.tenant))
            .iter()
            .map(|(_, _, _, count)| count)
            .sum();
        (nodes, edges)
    }

    pub(crate) fn new(
        view: Arc<OverlayView>,
        tenant: TenantId,
        text: Option<Arc<rushdb_textsearch::TextIndexes>>,
    ) -> Self {
        Self {
            view,
            tenant,
            failure: Mutex::new(None),
            families: Mutex::new(FamilyCache::default()),
            equality_indexes: Mutex::new(HashMap::new()),
            text,
            text_matches: Mutex::new(HashMap::new()),
            text_failure: Mutex::new(None),
        }
    }

    /// The first text search failure of the statement, if any.
    pub(crate) fn text_check(&self) -> Result<(), crate::LanguageError> {
        match self
            .text_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            Some(error) => Err(crate::LanguageError::TextSearch(error.clone())),
            None => Ok(()),
        }
    }

    /// The text index answering `text_match(n.<property>, …)` on `label`:
    /// the `ready` index on exactly that property (`*` for the index over
    /// every string of the record).
    fn text_index(&self, label: &str, property: &str) -> Option<String> {
        self.text.as_ref()?;
        self.read(|| {
            rushdb_textsearch::TextIndexes::index_for(
                self.view.as_ref(),
                self.tenant,
                label,
                property,
            )
        })
    }

    /// Record the statement's first text search failure and stop its work.
    fn text_failed(&self, error: String) {
        let mut failure = self
            .text_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if failure.is_none() {
            *failure = Some(error);
        }
        self.view.cancellation().cancel();
    }

    /// Run one search of `index`, recording a failure instead of returning it.
    fn text_hits(&self, index: &str, query: &str, top_k: usize) -> Vec<(u64, f64)> {
        let Some(text) = &self.text else {
            return Vec::new();
        };
        if self.text_check().is_err() {
            return Vec::new();
        }
        let found = text.search(
            self.tenant,
            self.view.base(),
            rushdb_textsearch::TextQuery {
                index,
                query,
                opts: rushdb_textsearch::LexicalOptions::default(),
                top_k,
            },
            None,
        );
        match found {
            Ok(found) => found
                .hits
                .into_iter()
                .map(|hit| (hit.owner_ord, hit.score))
                .collect(),
            Err(error) => {
                self.text_failed(error);
                Vec::new()
            }
        }
    }

    /// Every match of `query` in `index`, searched once per statement.
    fn text_matches(&self, index: &str, query: &str) -> Arc<HashMap<u64, f64>> {
        let key = (index.to_owned(), query.to_owned());
        if let Some(found) = self
            .text_matches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
        {
            return Arc::clone(found);
        }
        let found: Arc<HashMap<u64, f64>> = Arc::new(
            self.text_hits(index, query, rushdb_textsearch::ALL_HITS)
                .into_iter()
                .collect(),
        );
        self.text_matches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, Arc::clone(&found));
        found
    }

    pub(crate) fn view(&self) -> &Arc<OverlayView> {
        &self.view
    }

    pub(crate) fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// Run `reconstruct` in a session that keeps the statement's family
    /// cache.
    fn with_session<T>(
        &self,
        reconstruct: impl FnOnce(&mut ReconstructSession<'_, OverlayView>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {

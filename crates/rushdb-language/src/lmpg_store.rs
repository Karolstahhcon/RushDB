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
        let cache = std::mem::take(
            &mut *self
                .families
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let mut session = ReconstructSession::with_cache(self.view.as_ref(), self.tenant, cache);
        let result = reconstruct(&mut session);
        *self
            .families
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = session.into_cache();
        result
    }

    pub(crate) fn check(&self) -> Result<(), StorageError> {
        match self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    // Grafeo 0.5.42's GraphStore methods cannot return storage errors. Poison
    // the whole statement on first failure, stop further storage work, and
    // reject its result at every engine boundary. This adapter is private so
    // callers cannot accidentally accept its terminal placeholder values.
    pub(crate) fn read<T: Default>(&self, read: impl FnOnce() -> Result<T, StorageError>) -> T {
        if self.check().is_err() {
            return T::default();
        }
        match read() {
            Ok(value) => value,
            Err(error) => {
                let mut failure = self
                    .failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if failure.is_none() {
                    *failure = Some(error);
                }
                self.view.cancellation().cancel();
                T::default()
            }
        }
    }

    fn node(&self, id: NodeId) -> Option<Node> {
        self.read(|| self.try_node(id))
    }

    pub(crate) fn try_node(&self, id: NodeId) -> Result<Option<Node>, StorageError> {
        let Some(record) = self.with_session(|session| session.reconstruct_record(id.as_u64()))?
        else {
            return Ok(None);
        };
        node_of(id, &record)
    }

    /// One property of a record, decoding only that property's occurrences.
    fn try_node_property(&self, id: NodeId, name: &str) -> Result<Option<Value>, StorageError> {
        let selected = HashSet::from([name.to_owned()]);
        let Some(record) = self
            .with_session(|session| session.reconstruct_record_selected(id.as_u64(), &selected))?
        else {
            return Ok(None);
        };
        record
            .get(name)
            .map(|value| json_to_value_checked(value).map_err(|()| StorageError::CorruptLog))
            .transpose()
    }

    /// The ordered indexes that answer equality on `property` for every
    /// label holding it, or `None`. Only types whose equality the index
    /// encodes exactly qualify (not datetimes, which Grafeo compares as
    /// strings).
    fn equality_indexes(&self, property: &str) -> Option<Vec<IndexDef>> {
        if let Some(known) = self
            .equality_indexes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(property)
        {
            return known.clone();
        }
        let found = self.read(|| {
            let norm = rushdb_types::value::normalize_name(property);
            if norm.is_empty() || rushdb_types::value::is_reserved_name(&norm) {
                return Ok(None);
            }
            let typed =
                rushdb_lmpg::index::record_property_types(self.view.as_ref(), self.tenant, &norm)?;
            if typed.iter().any(|(ty, _)| *ty == LogicalType::DateTime) {
                return Ok(None);
            }
            let mut labels = BTreeSet::new();
            for (_, pid) in &typed {
                for (label, count) in
                    access::prop_support_label_counts(self.view.as_ref(), self.tenant, *pid)?
                {
                    if count > 0 {
                        labels.insert(label);
                    }
                }
            }
            let mut defs = Vec::with_capacity(labels.len());
            for label in labels {
                match rushdb_lmpg::index::ready_ordered(
                    self.view.as_ref(),
                    self.tenant,
                    &label,
                    &norm,
                )? {
                    Some(def) => defs.push(def),
                    None => return Ok(None),
                }
            }
            Ok(Some(defs))
        });
        self.equality_indexes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(property.to_owned(), found.clone());
        found
    }

    /// Owners whose `property` may equal `value`, from its ordered indexes,
    /// or `None` when they cannot answer (no index, or a value the index
    /// does not encode). A superset: the caller checks each owner.
    fn indexed_candidates(&self, property: &str, value: &Value) -> Option<BTreeSet<u64>> {
        let defs = self.equality_indexes(property)?;
        let probes = equality_probes(value)?;
        Some(self.read(|| {
            let mut owners = BTreeSet::new();
            for (start, end) in &probes {
                for def in &defs {
                    let rows = rushdb_lmpg::index::entries(
                        self.view.as_ref(),
                        self.tenant,
                        def.id,
                        start,
                        end,
                    )?;
                    owners.extend(rows.into_iter().map(|(_, owner)| owner));
                }
            }
            Ok(owners)
        }))
    }

    /// Records matching every `conditions` equality, exactly as Grafeo
    /// compares values.
    fn matching_nodes(&self, conditions: &[(&str, Value)]) -> Vec<NodeId> {
        if conditions.iter().any(|(_, value)| value.is_null()) {
            return Vec::new();
        }
        let candidates = conditions
            .iter()
            .find_map(|(property, value)| self.indexed_candidates(property, value))
            .map_or_else(
                || self.node_ids(),
                |owners| owners.into_iter().map(NodeId::new).collect(),
            );
        candidates
            .into_iter()
            .filter(|id| {
                conditions.iter().all(|(property, expected)| {
                    self.read(|| self.try_node_property(*id, property))
                        .is_some_and(|actual| values_equal(&actual, expected))
                })
            })
            .collect()
    }
}

/// A Grafeo node from a reconstructed record document.
pub(crate) fn node_of(id: NodeId, record: &Json) -> Result<Option<Node>, StorageError> {
    let object = record.as_object().ok_or(StorageError::CorruptLog)?;
    let label = object
        .get(rushdb_types::value::LABEL_FIELD)
        .and_then(Json::as_str)
        .ok_or(StorageError::CorruptLog)?;
    let mut node = Node::with_labels(id, [label]);
    for (key, value) in object {
        if !rushdb_types::value::is_reserved_name(key) {
            node.set_property(
                key.as_str(),
                json_to_value_checked(value).map_err(|()| StorageError::CorruptLog)?,
            );

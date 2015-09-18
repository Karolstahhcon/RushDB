//! The graph a graph-language mutation runs against: a copy-on-write layer
//! over the tenant's LMPG view. Reads fall through to LMPG; only the records
//! and relationships the statement creates or changes are held in memory,
//! so a write costs what it touches, not the size of the tenant. When the
//! statement succeeds, the layer's diff is the LMPG commit; when it fails,
//! the layer is dropped.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use grafeo_common::types::{EdgeId, EpochId, NodeId, PropertyKey, TransactionId, Value};
use grafeo_common::utils::hash::FxHashMap;
use grafeo_core::graph::lpg::{CompareOp, Edge, Node};
use grafeo_core::graph::{Direction, GraphStore, GraphStoreMut, GraphStoreSearch};
use grafeo_core::statistics::Statistics;
use rushdb_lmpg::reader::try_access as access;

use crate::lmpg_store::{values_equal, LmpgGraphStore};
use crate::mutation_budget::{property_bytes, MutationBudget};
use crate::{all_fields, changed_fields, LanguageError};

/// Ids of created nodes and edges start here, above every LMPG ordinal.
const CREATED_BASE: u64 = 1 << 62;

/// The base (LMPG) version of a node or edge the statement touched.
struct Original<T, R> {
    entity: T,
    reference: R,
}

#[derive(Default)]
struct State {
    /// Current version of every touched or created node; `None` once deleted.
    nodes: HashMap<NodeId, Option<Node>>,
    edges: HashMap<EdgeId, Option<Edge>>,
    original_nodes: BTreeMap<NodeId, Original<Node, rushdb_lmpg::RecordRef>>,
    original_edges: BTreeMap<EdgeId, Original<Edge, rushdb_lmpg::RelationshipRef>>,
    created_nodes: Vec<NodeId>,
    created_edges: Vec<EdgeId>,
    /// Adjacency of created edges.
    added_out: HashMap<NodeId, Vec<(NodeId, EdgeId)>>,
    added_in: HashMap<NodeId, Vec<(NodeId, EdgeId)>>,
    next_id: u64,
    /// Nodes touched or created, once that passed `max_nodes`.
    too_large: Option<usize>,
}

impl State {
    fn working_nodes(&self) -> usize {
        self.original_nodes.len() + self.created_nodes.len()
    }
}

/// The exact LMPG delta of a statement, with the layer ids of what it
/// creates (in batch order).
pub(crate) struct GraphDelta {
    pub(crate) batch: rushdb_lmpg::BatchTx,
    pub(crate) created_node_ids: Vec<NodeId>,
    pub(crate) created_edge_ids: Vec<EdgeId>,
}

pub(crate) struct OverlayStore {
    base: LmpgGraphStore,
    budget: MutationBudget,
    max_nodes: usize,
    state: RwLock<State>,
}

impl OverlayStore {
    pub(crate) fn new(base: LmpgGraphStore, max_nodes: usize, max_bytes: usize) -> Self {
        Self {
            base,
            budget: MutationBudget::new(max_bytes),
            max_nodes,
            state: RwLock::new(State {
                next_id: CREATED_BASE,
                ..State::default()
            }),
        }
    }

    pub(crate) fn budget(&self) -> &MutationBudget {
        &self.budget
    }

    fn read_state(&self) -> RwLockReadGuard<'_, State> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, State> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Every failure the statement hit: a storage error, the node bound, or
    /// the byte budget.
    pub(crate) fn check(&self) -> Result<(), LanguageError> {
        self.base.text_check()?;
        self.base.check()?;
        if let Some(nodes) = self.read_state().too_large {
            return Err(LanguageError::GraphTooLarge(nodes));
        }
        self.budget.check()
    }

    fn current_node(&self, id: NodeId) -> Option<Node> {
        if let Some(entry) = self.read_state().nodes.get(&id) {
            return entry.clone();
        }
        if id.as_u64() >= CREATED_BASE {
            return None;
        }
        self.base.get_node(id)
    }

    fn current_edge(&self, id: EdgeId) -> Option<Edge> {
        if let Some(entry) = self.read_state().edges.get(&id) {
            return entry.clone();
        }
        if id.as_u64() >= CREATED_BASE {
            return None;
        }
        self.base.get_edge(id)
    }

    /// Bring base node `id` into the layer; `false` when it does not exist,
    /// is deleted, or the working set is full.
    fn touch_node(&self, state: &mut State, id: NodeId) -> bool {
        if let Some(entry) = state.nodes.get(&id) {
            return entry.is_some();
        }
        if id.as_u64() >= CREATED_BASE || !self.admit_node(state) {
            return false;
        }
        let Some(node) = self.base.get_node(id) else {
            return false;
        };
        let header = self.base.read(|| {
            access::record_by_ord(self.base.view().as_ref(), self.base.tenant(), id.as_u64())
        });
        let Some(header) = header else {
            return false;
        };
        self.budget.admit(node_bytes(&node));
        state.nodes.insert(id, Some(node.clone()));
        state.original_nodes.insert(
            id,
            Original {
                entity: node,
                reference: rushdb_lmpg::RecordRef {
                    record_id: header.record_id,
                    ordinal: header.ordinal,
                },
            },
        );
        true
    }

    fn touch_edge(&self, state: &mut State, id: EdgeId) -> bool {
        if let Some(entry) = state.edges.get(&id) {
            return entry.is_some();
        }
        if id.as_u64() >= CREATED_BASE {
            return false;
        }
        let Some(edge) = self.base.get_edge(id) else {
            return false;
        };
        let header = self.base.read(|| {
            access::relationship_by_ord(self.base.view().as_ref(), self.base.tenant(), id.as_u64())
        });
        let Some(header) = header else {
            return false;
        };
        self.budget.admit(edge_bytes(&edge));
        state.edges.insert(id, Some(edge.clone()));
        state.original_edges.insert(
            id,
            Original {
                entity: edge,
                reference: rushdb_lmpg::RelationshipRef {
                    relationship_id: header.relationship_id,
                    ordinal: header.ordinal,
                },
            },
        );
        true
    }

    /// Count one more node in the working set (`max_nodes`).
    fn admit_node(&self, state: &mut State) -> bool {
        let next = state.working_nodes() + 1;
        if next > self.max_nodes {
            state.too_large.get_or_insert(next);
            return false;
        }
        true
    }

    fn next_id(state: &mut State) -> u64 {
        let id = state.next_id;
        state.next_id += 1;
        id
    }

    /// Ids the overlay overrides: the base answer for them is stale.
    fn touched_nodes(&self) -> Vec<(NodeId, Option<Node>)> {
        self.read_state()
            .nodes
            .iter()
            .map(|(id, node)| (*id, node.clone()))
            .collect()
    }

    /// Base ids corrected by the layer: overridden ids are dropped, and the
    /// touched or created nodes `keep` accepts are added.
    fn corrected(&self, base: Vec<NodeId>, keep: impl Fn(&Node) -> bool) -> Vec<NodeId> {
        let touched = self.touched_nodes();
        let overridden: std::collections::HashSet<NodeId> =
            touched.iter().map(|(id, _)| *id).collect();
        let mut out: Vec<NodeId> = base
            .into_iter()
            .filter(|id| !overridden.contains(id))
            .collect();
        out.extend(
            touched
                .into_iter()
                .filter_map(|(id, node)| node.filter(|node| keep(node)).map(|_| id)),
        );
        out.sort_unstable();
        out
    }

    /// The LMPG delta of everything the statement did.
    pub(crate) fn delta(&self) -> Result<GraphDelta, LanguageError> {
        self.check()?;
        let state = self.read_state();
        let mut batch = rushdb_lmpg::BatchTx { ops: Vec::new() };
        Self::changes_to_originals(&state, &mut batch, |id| self.current_node_in(&state, id))?;
        let (created_node_ids, node_public_ids) = Self::created_nodes(&state, &mut batch)?;
        Self::edge_updates(&state, &mut batch);
        let mut created_edge_ids = Vec::new();
        for id in &state.created_edges {
            let Some(Some(edge)) = state.edges.get(id) else {
                continue;
            };
            let endpoint = |node: NodeId, role: &str| -> Result<[u8; 16], LanguageError> {
                if self.current_node_in(&state, node).is_none() {
                    return Err(LanguageError::UnsupportedMutation(format!(
                        "new relationship {role} was deleted"
                    )));
                }
                match node_public_ids.get(&node) {
                    Some(id) => Ok(*id),
                    None => self.base_record_id(&state, node),
                }
            };
            let source_id = endpoint(edge.src, "source")?;
            let target_id = endpoint(edge.dst, "target")?;
            created_edge_ids.push(*id);
            batch
                .ops
                .push(rushdb_lmpg::BatchOp::CreateRelationshipById {
                    rel_type: edge.edge_type.to_string(),
                    source_id,
                    target_id,
                    fields: all_fields(&edge.properties_as_btree()),
                });
        }
        drop(state);
        self.check()?;
        Ok(GraphDelta {
            batch,
            created_node_ids,
            created_edge_ids,
        })
    }

    /// Field updates of touched records (label changes are refused), then
    /// standalone relationship deletes, then record deletes (a deleted
    /// record's relationships go with it: LMPG cascades).
    fn changes_to_originals(
        state: &State,
        batch: &mut rushdb_lmpg::BatchTx,
        current_node: impl Fn(NodeId) -> Option<Node>,
    ) -> Result<(), LanguageError> {
        for (id, original) in &state.original_nodes {
            let Some(Some(node)) = state.nodes.get(id) else {
                continue;
            };
            if labels_of(node) != labels_of(&original.entity) {
                return Err(LanguageError::UnsupportedMutation(
                    "LMPG records have one primary label; graph-statement label mutation is unavailable (use native retypeRecord)".into(),
                ));
            }
            let fields = changed_fields(
                &original.entity.properties_as_btree(),
                &node.properties_as_btree(),
            );
            if !fields.is_empty() {
                batch.ops.push(rushdb_lmpg::BatchOp::UpdateFields {
                    owner: rushdb_types::ids::OwnerKind::Record,
                    owner_ord: original.reference.ordinal,
                    owner_public_id: original.reference.record_id,
                    fields,
                });
            }
        }

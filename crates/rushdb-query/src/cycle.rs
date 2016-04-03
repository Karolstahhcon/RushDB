//! `$cycle`: does a record lie on a closed trail (a path back to itself that
//! uses no relationship twice) of `min..=max` hops over relationships of one
//! type and direction?
//!
//! Enumerating trails per record is exponential in the hop count. Instead,
//! one graph per query and relationship shape is built from the adjacency
//! rows and split into the parts a cycle can live in (strongly connected
//! components for a direction; for `both`, components minus bridges). Per
//! record, a breadth-first search finds its shortest cycle `s`, which is
//! always a simple cycle and so a trail: `s` in range answers `true`; no
//! cycle within `max` hops answers `false`. Only when `s < min` must a longer
//! trail be searched for, depth-first and within a step budget that fails
//! the query with a resource error instead of running unbounded.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use rushdb_storage::{ReadView, ScanBounds, ScanProjection};
use rushdb_types::ids::TenantId;

use crate::{Direction, QueryError, RelatedExpr};

/// Trail expansions a `$cycle` check may spend searching for a cycle longer
/// than the record's shortest one.
const TRAIL_SEARCH_BUDGET: usize = 100_000;

/// Graphs already built for one query, by relationship type and direction.
type GraphCache = HashMap<(Option<String>, u8), Arc<CycleGraph>>;

/// Graphs built for one query, keyed by relationship type and direction.
#[derive(Default)]
pub(crate) struct CycleGraphs(RefCell<GraphCache>);

impl CycleGraphs {
    /// Whether the record `ord` lies on a closed trail matching the `$cycle`
    /// block `rel` (its type, direction and hops).
    pub(crate) fn on_cycle(
        &self,
        snap: &ReadView,
        tenant: TenantId,
        rel: &RelatedExpr,
        ord: u64,
        cancel: &dyn Fn() -> Result<(), QueryError>,
    ) -> Result<bool, QueryError> {
        let key = (
            rel.rel_type.clone(),
            match rel.direction {
                Direction::Outgoing => 0,
                Direction::Incoming => 1,
                Direction::Both => 2,
            },
        );
        let cached = self.0.borrow().get(&key).cloned();
        let graph = if let Some(graph) = cached {
            graph
        } else {
            let graph = Arc::new(CycleGraph::build(
                snap,
                tenant,
                rel.rel_type.as_deref(),
                rel.direction,
                cancel,
            )?);
            self.0.borrow_mut().insert(key, Arc::clone(&graph));
            graph
        };
        let (min, max) = rel.hops.unwrap_or((1, 1));
        graph.on_cycle(ord, min, max, cancel)
    }
}

/// The relationships of one type, as traversed in one direction.
pub(crate) struct CycleGraph {
    index: HashMap<u64, u32>,
    /// Per node: `(neighbour, relationship ordinal)` in traversal direction.
    adj: Vec<Vec<(u32, u64)>>,
    /// Strongly connected component (directed) or connected component (`both`).
    component: Vec<u32>,
    /// Whether the node can lie on any cycle at all.
    capable: Vec<bool>,
    undirected: bool,
}

impl CycleGraph {
    fn build(
        snap: &ReadView,
        tenant: TenantId,
        rel_type: Option<&str>,
        direction: Direction,
        cancel: &dyn Fn() -> Result<(), QueryError>,
    ) -> Result<Self, QueryError> {
        let mut graph = CycleGraph {
            index: HashMap::new(),
            adj: Vec::new(),
            component: Vec::new(),
            capable: Vec::new(),
            undirected: direction == Direction::Both,
        };
        // Every relationship has one outgoing adjacency row:
        // `T/adj/out/<source:8>/<type>/<target:8>/<rel:8>`.
        let prefix = rushdb_types::keys::key(tenant, &[b"adj", b"out", b""]);
        let mut cursor = snap.scan(ScanBounds::prefix(&prefix), ScanProjection::Keys)?;
        loop {
            cancel()?;
            let batch = cursor.next_batch(4_096, 1 << 20)?;
            for row in batch.rows {
                let Some(rest) = row.key.strip_prefix(prefix.as_slice()) else {
                    continue;
                };
                if rest.len() < 8 + 1 + 1 + 18 {
                    continue;
                }
                let source = u64::from_be_bytes(rest[..8].try_into().unwrap_or_default());
                let tail = &rest[rest.len() - 17..];
                let name = &rest[9..rest.len() - 18];
                if rel_type.is_some_and(|wanted| wanted.as_bytes() != name) {
                    continue;
                }
                let target = u64::from_be_bytes(tail[..8].try_into().unwrap_or_default());
                let rel = u64::from_be_bytes(tail[9..17].try_into().unwrap_or_default());
                let (from, to) = match direction {
                    Direction::Outgoing | Direction::Both => (source, target),
                    Direction::Incoming => (target, source),
                };
                let (from, to) = (graph.node(from), graph.node(to));
                graph.adj[from as usize].push((to, rel));
                if graph.undirected && from != to {
                    graph.adj[to as usize].push((from, rel));
                }
            }
            if batch.done {
                break;
            }
        }
        if graph.undirected {
            graph.mark_undirected();
        } else {
            graph.mark_directed();
        }
        Ok(graph)
    }

    fn node(&mut self, ord: u64) -> u32 {
        let next = u32::try_from(self.adj.len()).unwrap_or(u32::MAX);
        let index = *self.index.entry(ord).or_insert(next);
        if index == next {
            self.adj.push(Vec::new());
        }
        index
    }

    /// Strongly connected components (iterative Tarjan); a node can be on a
    /// cycle when its component has two or more nodes or it has a self-loop.
    fn mark_directed(&mut self) {
        let n = self.adj.len();
        let unset = u32::MAX;
        let (mut index, mut low) = (vec![unset; n], vec![0u32; n]);
        let mut on_stack = vec![false; n];
        let (mut stack, mut component) = (Vec::new(), vec![unset; n]);
        let (mut next_index, mut next_component) = (0u32, 0u32);
        for root in 0..n {
            if index[root] != unset {
                continue;
            }
            let mut work: Vec<(usize, usize)> = vec![(root, 0)];
            index[root] = next_index;
            low[root] = next_index;
            next_index += 1;
            stack.push(root);
            on_stack[root] = true;
            while let Some(&mut (v, ref mut at)) = work.last_mut() {
                if let Some(&(w, _)) = self.adj[v].get(*at) {
                    *at += 1;
                    let w = w as usize;
                    if index[w] == unset {
                        index[w] = next_index;
                        low[w] = next_index;
                        next_index += 1;
                        stack.push(w);
                        on_stack[w] = true;
                        work.push((w, 0));
                    } else if on_stack[w] {
                        low[v] = low[v].min(index[w]);
                    }
                    continue;
                }
                work.pop();
                if let Some(&(parent, _)) = work.last() {
                    low[parent] = low[parent].min(low[v]);
                }
                if low[v] == index[v] {
                    while let Some(w) = stack.pop() {
                        on_stack[w] = false;
                        component[w] = next_component;
                        if w == v {
                            break;
                        }
                    }
                    next_component += 1;
                }
            }
        }

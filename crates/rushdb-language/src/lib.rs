//! Standards-based query-language frontends for `RushDB`.
//!
//! Grafeo owns parsing, binding, optimization, and the shared logical plan for
//! ISO GQL, openCypher, Gremlin, and SQL/PGQ. `LmpgGraphStore` is deliberately
//! a thin, tenant-scoped storage adapter: it contains no product/business
//! semantics and cannot see data outside the supplied `Lmpg` tenant.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use grafeo_common::types::{EdgeId, LogicalType, NodeId, PropertyKey, Value};
use grafeo_core::graph::{GraphStore, GraphStoreMut, GraphStoreSearch};
use grafeo_engine::{Config, GrafeoDB};
use rushdb_lmpg::reconstruct::{reconstruct_record, reconstruct_relationship};
use rushdb_lmpg::Lmpg;
use rushdb_storage::{OverlayView, ReadView, StorageError};
use rushdb_types::ids::TenantId;
use rushdb_types::value::{ID_FIELD, LABEL_FIELD, SOURCE_FIELD, TARGET_FIELD, TYPE_FIELD};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as Json};

#[cfg(test)]
mod pinned_tests;

/// Neo4j-style index DDL recognised before translation.
pub mod ddl;
/// Gremlin traversal bytecode (`GraphSON` 3) to Gremlin text.
pub mod gremlin_bytecode;
mod lmpg_store;
mod mutation_budget;
mod overlay;
use lmpg_store::LmpgGraphStore;
use overlay::OverlayStore;

/// P10 bounded-mutation budget: the maximum tenant graph size (nodes) that a
/// single graph-language mutation is allowed to clone into the community
/// engine's in-memory working set. Larger tenants are refused at load time so
/// a small mutation never silently copies the whole tenant.
pub const MUTATION_MAX_NODES: usize = 100_000;

/// Default cumulative admission ceiling for graph mutation staging.
pub const MUTATION_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Stack of the thread a statement runs on. Grafeo parses and plans
/// recursively, about one frame chain per clause, so a long statement (a
/// 500-clause `CREATE` script) overflows an ordinary worker stack, which
/// aborts the process. Only the pages a statement touches are committed.
const LANGUAGE_STACK_BYTES: usize = 256 * 1024 * 1024;

/// Clause keywords one statement may hold. Measured: 16,000 `CREATE`
/// clauses parse and plan within [`LANGUAGE_STACK_BYTES`] (release and dev
/// builds); the bound keeps a 2x margin, so no statement can exhaust the
/// stack.
pub const MAX_STATEMENT_CLAUSES: usize = 8_192;

/// Clause keywords and traversal steps of `statement`, outside string
/// literals: a proxy for the recursion depth its parse and plan need.
fn clause_count(statement: &str) -> usize {
    const KEYWORDS: [&str; 16] = [
        "MATCH", "CREATE", "MERGE", "SET", "DELETE", "REMOVE", "WITH", "UNWIND", "RETURN", "CALL",
        "FOREACH", "UNION", "INSERT", "FILTER", "LET", "FOR",
    ];
    let tokens = tokens(statement);
    tokens
        .words
        .iter()
        .filter(|word| {
            KEYWORDS
                .iter()
                .any(|keyword| keyword.eq_ignore_ascii_case(word))
        })
        .count()
        + tokens.steps
}

/// The words of a statement outside string literals, and its Gremlin steps
/// (`).name(`).
struct Tokens {
    words: Vec<String>,
    steps: usize,
}

fn tokens(statement: &str) -> Tokens {
    let mut out = Tokens {
        words: Vec::new(),
        steps: 0,
    };
    let mut word = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let mut previous = ' ';
    for c in statement.chars() {
        if let Some(open) = quote {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == open {
                quote = None;
            }
            continue;
        }
        if c.is_alphanumeric() || c == '_' {
            word.push(c);
        } else {
            if !word.is_empty() {
                out.words.push(std::mem::take(&mut word));
            }
            if c == '\'' || c == '"' || c == '`' {
                quote = Some(c);
            } else if c == '.' && previous == ')' {
                out.steps += 1;
            }
        }
        if !c.is_whitespace() {
            previous = c;
        }
    }
    if !word.is_empty() {
        out.words.push(word);
    }
    out
}

/// Run one statement's parse, plan and execution on a thread with
/// [`LANGUAGE_STACK_BYTES`] of stack, refusing a statement longer than
/// [`MAX_STATEMENT_CLAUSES`] first.
///
/// Grafeo cannot be interrupted mid-operator, so the calling thread watches
/// `is_cancelled` (the request's cancellation and deadline) while the
/// statement runs, and trips `cancel` (the statement's read views) when it
/// fires: every following read fails, which ends any operator that reads
/// the graph, including one that would never finish.
fn on_language_stack<T: Send>(
    statement: &str,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    cancel: &rushdb_storage::ReadCancellation,
    run: impl FnOnce() -> Result<T, LanguageError> + Send,
) -> Result<T, LanguageError> {
    let clauses = clause_count(statement);
    if clauses > MAX_STATEMENT_CLAUSES {
        return Err(LanguageError::StatementTooLong {
            clauses,
            limit: MAX_STATEMENT_CLAUSES,
        });
    }
    let caller = std::thread::current();
    std::thread::scope(|scope| {
        let worker = std::thread::Builder::new()
            .name("rushdb-language".into())
            .stack_size(LANGUAGE_STACK_BYTES)
            .spawn_scoped(scope, move || {
                let result = run();
                // Wake the watching caller now, not at its next poll.
                caller.unpark();
                result
            })
            .map_err(|error| {
                LanguageError::Initialization(format!("cannot start a statement thread: {error}"))
            })?;
        let mut tripped = false;
        while !worker.is_finished() {
            if !tripped && is_cancelled() {
                cancel.cancel();
                tripped = true;
            }
            std::thread::park_timeout(CANCELLATION_POLL);
        }
        let result = worker
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        if tripped {
            // A text search records its failure and cancels the view to stop
            // further reads. The poll above can observe that cancellation
            // before the worker returns, so the recorded text error is the
            // real outcome and wins over the cancellation it caused.
            if let Err(LanguageError::TextSearch(_)) = &result {
                return result;
            }
            return Err(LanguageError::Cancelled);
        }
        result
    })
}

/// How often a running statement's cancellation is checked.
const CANCELLATION_POLL: std::time::Duration = std::time::Duration::from_millis(5);

/// A standards-based graph query language frontend accepted by this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryLanguage {
    /// ISO/IEC 39075 GQL.
    Gql,
    /// openCypher.
    Cypher,
    /// Apache `TinkerPop` Gremlin.
    Gremlin,
    /// SQL:2023 property-graph queries (`GRAPH_TABLE`). General relational
    /// SQL remains the responsibility of the relational SQL frontend.
    SqlPgq,
}

/// A materialized graph-query result set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LanguageResult {
    /// Projected column names, in projection order.
    pub columns: Vec<String>,
    /// One JSON object per result row, keyed by column name.
    pub rows: Vec<Json>,
    /// Optional engine status message (for example, a server-side notice).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// The outcome of a committed graph-language mutation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LanguageMutationResult {
    /// The statement's materialized result rows.
    pub result: LanguageResult,
    /// Number of graph operations published in the commit.
    pub affected: u64,
    /// LMPG commit index at which the mutation was published.
    pub commit_index: u64,
}

/// Errors raised while reading, executing, or committing a graph statement.
#[derive(Debug, thiserror::Error)]
pub enum LanguageError {
    /// A tenant storage read failed.
    #[error("query storage read failed: {0}")]
    Storage(#[from] StorageError),
    /// The community query engine could not be initialized.
    #[error("query engine initialization failed: {0}")]
    Initialization(String),
    /// The query engine rejected the statement.
    #[error("{language:?} query failed: {message}")]
    Query {
        /// Language that was executing when the failure occurred.
        language: QueryLanguage,
        /// Engine error text.
        message: String,
    },
    /// A text search in the statement failed (unknown index, unparsable
    /// query).
    #[error("text search failed: {0}")]
    TextSearch(String),
    /// Parameters were supplied but were not a JSON object.
    #[error("query parameters must be a JSON object")]
    InvalidParameters,
    /// The requested mutation is not supported by this frontend.
    #[error("query-language mutation is unsupported: {0}")]
    UnsupportedMutation(String),
    /// Another commit wrote the tenant after the statement's snapshot; the
    /// statement can be retried.
    #[error("query-language write conflict: {0}")]
    WriteConflict(String),
    /// The derived delta could not be committed atomically.
    #[error("query-language mutation failed to commit atomically: {0}")]
    Commit(String),
    /// P10: a mutation's working set exceeds the bounded-mutation budget, so
    /// the whole-tenant graph copy is refused instead of silently cloning an
    /// oversized tenant. The node count that triggered the refusal is carried.
    #[error("query-language mutation graph exceeds the bounded budget: {0} nodes")]
    GraphTooLarge(usize),
    /// Disposable graph or output exceeded cumulative byte admission.
    #[error("resource exhausted: graph mutation staging exceeds {0} bytes")]
    MutationBytesExceeded(usize),
    /// The statement has more clauses than one statement may hold
    /// ([`MAX_STATEMENT_CLAUSES`]); split it.
    #[error("statement has {clauses} clauses; at most {limit} are accepted, split it")]
    StatementTooLong {
        /// Clauses counted.
        clauses: usize,
        /// The limit.
        limit: usize,
    },
    /// The caller cancelled or the deadline elapsed during execution. Polled
    /// before execution and between converted result rows.
    #[error("query execution cancelled")]
    Cancelled,
}

/// Execute one standards-based graph statement against the current tenant.
///
/// The external store is read-only on purpose. A write statement is rejected
/// by the query engine rather than partially committing multiple LMPG changes.
/// Write support must be connected to `RushDB`'s atomic transaction coordinator.
///
/// # Errors
///
/// Returns [`LanguageError::Storage`] if the tenant snapshot cannot be read,
/// [`LanguageError::Initialization`] if the community engine cannot be built,
/// [`LanguageError::InvalidParameters`] if `parameters` is not a JSON object,
/// and [`LanguageError::Query`] if the query engine rejects the statement.
pub fn execute(
    lmpg: &Lmpg,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
) -> Result<LanguageResult, LanguageError> {
    let snapshot = Arc::new(lmpg.store.read_view()?);
    execute_in_view(&snapshot, lmpg.tenant, language, statement, parameters)
}

/// Classify an openCypher statement from its parsed logical plan before any
/// execution. This is the shared authority for transport adapters deciding
/// whether a statement must use the mutation envelope; adapters must not
/// infer write intent by searching source text.
///
/// # Errors
/// Returns the parser/translator error when the statement is not valid
/// openCypher.
pub fn classify_cypher_mutation(statement: &str) -> Result<bool, String> {
    use grafeo_engine::query::translators::cypher::{translate_full, CypherTranslationResult};

    let translated = translate_full(statement).map_err(|error| error.to_string())?;
    Ok(match translated {
        CypherTranslationResult::Plan(plan) => plan.root.has_mutations(),
        CypherTranslationResult::ShowIndexes
        | CypherTranslationResult::ShowConstraints
        | CypherTranslationResult::ShowCurrentGraphType => false,
        _ => true,
    })
}

/// Execute a graph-language read against a caller-owned pinned snapshot.
///
/// Transport adapters use this entry point when they already created an
/// execution context. The adapter never opens a second view, so every graph
/// read observes the same commit as the surrounding native operation.
///
/// # Errors
/// Returns the same storage, initialization, parameter, and query errors as
/// [`execute`].
pub fn execute_in_view(
    snapshot: &Arc<ReadView>,
    tenant: TenantId,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
) -> Result<LanguageResult, LanguageError> {
    execute_in_view_with_budget(snapshot, tenant, language, statement, parameters, None)
}

/// Execute a graph-language read over a pinned snapshot with an optional
/// bounded JSON result working set. Each converted row is admitted before it
/// is retained in the returned result vector.
///
/// # Errors
/// Returns the same errors as [`execute_in_view`], plus a query error when the
/// converted result rows exceed `max_output_bytes`.
pub fn execute_in_view_with_budget(
    snapshot: &Arc<ReadView>,
    tenant: TenantId,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_output_bytes: Option<usize>,
) -> Result<LanguageResult, LanguageError> {
    execute_in_view_with_budget_cancellable(
        snapshot,
        tenant,
        language,
        statement,
        parameters,
        max_output_bytes,
        &|| false,
        None,
    )
}

/// Cancellation-aware form of [`execute_in_view_with_budget`]. `is_cancelled`
/// is polled before the (synchronous) community-engine call and between
/// converted result rows, so an oversized result stops promptly. The Grafeo
/// call itself cannot be interrupted mid-execution.
///
/// # Errors
/// Returns the same errors as [`execute_in_view_with_budget`], plus
/// [`LanguageError::Cancelled`].
#[expect(
    clippy::too_many_arguments,
    reason = "the statement, its pinned view, budgets, the request's cancellation and the node's text indexes"
)]
pub fn execute_in_view_with_budget_cancellable(
    snapshot: &Arc<ReadView>,
    tenant: TenantId,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_output_bytes: Option<usize>,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<LanguageResult, LanguageError> {
    on_language_stack(statement, is_cancelled, &snapshot.cancellation(), || {
        execute_in_view_with_budget_cancellable_on_stack(
            snapshot,
            tenant,
            language,
            statement,
            parameters,
            max_output_bytes,
            is_cancelled,
            text,
        )
    })
}

/// The body of [`execute_in_view_with_budget_cancellable`], on the language stack.
#[expect(
    clippy::too_many_arguments,
    reason = "the statement, its pinned view, budgets, the request's cancellation and the node's text indexes"
)]
fn execute_in_view_with_budget_cancellable_on_stack(
    snapshot: &Arc<ReadView>,
    tenant: TenantId,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_output_bytes: Option<usize>,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<LanguageResult, LanguageError> {
    if is_cancelled() {
        return Err(LanguageError::Cancelled);
    }
    let adapter = Arc::new(LmpgGraphStore::new(
        Arc::new(OverlayView::new(Arc::clone(snapshot))),
        tenant,
        text.cloned(),
    ));
    let store: Arc<dyn GraphStoreSearch> = adapter.clone();
    let initialized = GrafeoDB::with_read_store(store, Config::in_memory());
    // A failed text search cancels the statement's reads: report it, not
    // the cancelled reads it caused.
    adapter.text_check()?;
    adapter.check()?;
    let db = initialized.map_err(|error| LanguageError::Initialization(error.to_string()))?;
    let params = parameters.map(parameters_from_json).transpose()?;
    let session = db.session();
    let language_name = match language {
        QueryLanguage::Gql => "gql",
        QueryLanguage::Cypher => "cypher",
        QueryLanguage::Gremlin => "gremlin",
        QueryLanguage::SqlPgq => "sql",
    };
    let executed = session.execute_language(statement, language_name, params);
    // A failed text search cancels the statement's reads: report it, not
    // the cancelled reads it caused.
    adapter.text_check()?;
    adapter.check()?;
    let result = executed.map_err(|error| LanguageError::Query {
        language,
        message: error.to_string(),
    })?;

    let columns = result.columns.clone();
    let column_types = result.column_types.clone();
    let status = result.status_message.clone();
    let materialized = result.into_rows();
    // A failed text search cancels the statement's reads: report it, not
    // the cancelled reads it caused.
    adapter.text_check()?;
    adapter.check()?;
    if returns_nothing(language, statement, &columns) {
        return Ok(LanguageResult {
            columns: Vec::new(),
            rows: Vec::new(),
            status,
        });
    }
    let mut rows = Vec::with_capacity(materialized.len());
    let mut output_bytes = 0usize;
    for row in materialized {
        if is_cancelled() {
            return Err(LanguageError::Cancelled);
        }
        let mut object = JsonMap::new();
        for (index, value) in row.into_iter().enumerate() {
            let name = columns
                .get(index)
                .cloned()
                .unwrap_or_else(|| format!("column_{index}"));
            let value = result_value_to_json(snapshot, tenant, column_types.get(index), value)?;
            object.insert(name, value);
        }
        let row = Json::Object(object);
        let row_bytes = serde_json::to_vec(&row)
            .map_err(|error| LanguageError::Query {
                language,
                message: format!("encode graph result row: {error}"),
            })?
            .len();
        output_bytes = output_bytes.saturating_add(row_bytes);
        if max_output_bytes.is_some_and(|limit| output_bytes > limit) {
            return Err(LanguageError::Query {
                language,
                message: "graph result exceeds execution byte budget".into(),
            });
        }
        rows.push(row);
    }
    // A failed text search cancels the statement's reads: report it, not
    // the cancelled reads it caused.
    adapter.text_check()?;
    adapter.check()?;

    Ok(LanguageResult {
        columns,
        rows,
        status,
    })
}

/// Execute a graph-language write in an isolated community-engine snapshot,
/// derive its exact graph delta, and publish that delta as one LMPG commit.
/// If the LMPG snapshot changes while Grafeo is executing, the commit fails
/// instead of overwriting a concurrent update.
///
/// # Errors
///
/// Propagates every error documented by
/// [`execute_mutation_with_budget`], plus [`LanguageError::Commit`] if the
/// derived delta cannot be published atomically.
pub fn execute_mutation(
    lmpg: &Lmpg,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
) -> Result<LanguageMutationResult, LanguageError> {
    execute_mutation_with_budget(lmpg, language, statement, parameters, MUTATION_MAX_NODES)
}

/// Execute a mutation with an explicit bounded-mutation budget. The
/// tenant graph is copied into the community engine's working set only up to
/// `max_nodes`; a larger tenant is refused with [`LanguageError::GraphTooLarge`]
/// before any copy, so a small mutation never silently clones an oversized
/// tenant.
///
/// # Errors
///
/// Returns [`LanguageError::GraphTooLarge`] if the tenant graph or the
/// mutation's created nodes exceed `max_nodes`,
/// [`LanguageError::UnsupportedMutation`] for SQL/PGQ or a statement that
/// changes nothing, [`LanguageError::Storage`] on read failure, and
/// [`LanguageError::Initialization`], [`LanguageError::InvalidParameters`],
/// [`LanguageError::Query`], or [`LanguageError::Commit`] if initialization,
/// parameter decoding, execution, or publication fails.
pub fn execute_mutation_with_budget(
    lmpg: &Lmpg,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_nodes: usize,
) -> Result<LanguageMutationResult, LanguageError> {
    let source_view = Arc::new(lmpg.store.read_view()?);
    execute_mutation_in_view_with_budget(
        lmpg,
        &source_view,
        language,
        statement,
        parameters,
        max_nodes,
    )
}

/// Execute a graph-language mutation against a caller-owned pinned view.
/// The view is used for the complete working-set load and its commit index is
/// used as the optimistic publication base, so transport callers cannot
/// silently switch snapshots mid-request.
///
/// # Errors
///
/// Returns the same bounded graph, parsing, storage, execution, and commit
/// errors as [`execute_mutation_with_budget`].
pub fn execute_mutation_in_view_with_budget(
    lmpg: &Lmpg,
    source_view: &Arc<rushdb_storage::ReadView>,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_nodes: usize,
) -> Result<LanguageMutationResult, LanguageError> {
    execute_mutation_in_view_with_limits(
        lmpg,
        source_view,
        language,
        statement,
        parameters,
        max_nodes,
        MUTATION_MAX_BYTES,
        &|| false,
        None,
    )
}

/// Execute a graph mutation with explicit node and cumulative byte ceilings.
/// Provider-private temporary allocations are outside this estimate; all
/// retained graph writes and output are admitted before publication.
/// `is_cancelled` is watched while the statement runs (see
/// [`on_language_stack`]).
///
/// # Errors
/// Returns the same errors as [`execute_mutation_in_view_with_budget`], plus
/// [`LanguageError::MutationBytesExceeded`] if admission fails.
#[expect(
    clippy::too_many_arguments,
    reason = "the statement, its pinned view, both admission limits and the request's cancellation"
)]
pub fn execute_mutation_in_view_with_limits(
    lmpg: &Lmpg,
    source_view: &Arc<ReadView>,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_nodes: usize,
    max_bytes: usize,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<LanguageMutationResult, LanguageError> {
    on_language_stack(statement, is_cancelled, &source_view.cancellation(), || {
        execute_mutation_in_view_with_limits_on_stack(
            lmpg,
            source_view,
            language,
            statement,
            parameters,
            max_nodes,
            max_bytes,
            text,
        )
    })
}

/// The body of [`execute_mutation_in_view_with_limits`], on the language stack.
#[expect(
    clippy::too_many_lines,
    reason = "one linear execute->delta path; splitting scatters the shared budget and snapshot borrows"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "the statement, its pinned view, budgets, the request's cancellation and the node's text indexes"
)]
fn execute_mutation_in_view_with_limits_on_stack(
    lmpg: &Lmpg,
    source_view: &Arc<ReadView>,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_nodes: usize,
    max_bytes: usize,
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<LanguageMutationResult, LanguageError> {
    if language == QueryLanguage::SqlPgq {
        return Err(LanguageError::UnsupportedMutation(
            "SQL/PGQ is a read-only SQL:2023 GRAPH_TABLE profile".into(),
        ));
    }

    let snapshot_version = source_view.commit_index();
    // The statement runs against a copy-on-write layer over the pinned view:
    // only what it touches is copied, bounded by `max_nodes` and `max_bytes`.
    let overlay = Arc::new(OverlayView::new(Arc::clone(source_view)));
    let store = Arc::new(OverlayStore::new(
        LmpgGraphStore::new(overlay, lmpg.tenant, text.cloned()),
        max_nodes,
        max_bytes,
    ));
    let graph_store: Arc<dyn GraphStoreMut> = store.clone();
    let db = GrafeoDB::with_store(graph_store, Config::in_memory())
        .map_err(|error| LanguageError::Initialization(error.to_string()))?;
    let params = parameters.map(parameters_from_json).transpose()?;
    let session = db.session();
    let language_name = match language {
        QueryLanguage::Gql => "gql",
        QueryLanguage::Cypher => "cypher",
        QueryLanguage::Gremlin => "gremlin",
        QueryLanguage::SqlPgq => unreachable!(),
    };
    // The community engine may perform several internal mutations, but they
    // touch only the layer. LMPG sees exactly one atomic diff after the
    // complete statement succeeds.
    let result = session.execute_language(statement, language_name, params);
    store.check()?;
    let result = result.map_err(|error| LanguageError::Query {
        language,
        message: error.to_string(),
    })?;

    let delta = store.delta()?;
    let affected = delta.batch.ops.len() as u64;
    let mut columns = result.columns.clone();
    let column_types = result.column_types.clone();
    let status = result.status_message.clone();
    let mut result_rows = result.into_rows();
    if returns_nothing(language, statement, &columns) {
        columns.clear();
        result_rows.clear();
    }
    for row in &result_rows {
        for value in row {
            store
                .budget()
                .admit(128usize.saturating_add(value.estimated_size_bytes()));
        }
    }
    store.check()?;
    let created_node_ids = delta.created_node_ids;
    let created_edge_ids = delta.created_edge_ids;
    // A write that changes nothing (a MERGE that matched, a SET over no
    // rows) succeeds without a commit, at the snapshot it read.
    let committed = if affected == 0 {
        rushdb_lmpg::CommittedBatch {
            commit_index: snapshot_version,
            created_records: Vec::new(),
            created_relationships: Vec::new(),
        }
    } else {
        lmpg.run_batch_at_version(delta.batch, snapshot_version)
            .map_err(|error| match error {
                rushdb_lmpg::LmpgError::WriteConflict(reason) => {
                    LanguageError::WriteConflict(reason)
                }
                other => LanguageError::Commit(other.to_reason()),
            })?
    };

    // Created record ids were chosen before the commit so relationships in
    // the same batch could resolve them; the committed values are
    // authoritative.
    let node_public_ids: HashMap<NodeId, [u8; 16]> = created_node_ids
        .into_iter()
        .zip(
            committed
                .created_records
                .iter()
                .map(|record| record.record_id),
        )
        .collect();
    let edge_public_ids: HashMap<EdgeId, [u8; 16]> = created_edge_ids
        .into_iter()
        .zip(
            committed
                .created_relationships
                .iter()
                .map(|relationship| relationship.relationship_id),
        )
        .collect();
    let node_id = |id: NodeId| store.public_node_id(id, &node_public_ids);
    let edge_id = |id: EdgeId| store.public_edge_id(id, &edge_public_ids);

    let rows = result_rows
        .into_iter()
        .map(|row| {
            let mut object = JsonMap::new();
            for (index, value) in row.into_iter().enumerate() {
                let name = columns
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| format!("column_{index}"));
                object.insert(
                    name,
                    mutable_result_value_to_json(
                        store.as_ref(),
                        &node_id,
                        &edge_id,
                        column_types.get(index),
                        value,
                    ),
                );
            }
            Json::Object(object)
        })
        .collect();

    Ok(LanguageMutationResult {
        result: LanguageResult {
            columns,
            rows,
            status,
        },
        affected,
        commit_index: committed.commit_index,
    })
}

/// Execute a mutation against a committed snapshot and return the exact LMPG
/// delta batch WITHOUT committing (cross-service transaction staging). The
/// caller applies each operation into a transaction's overlay; the writes then
/// commit or roll back with the transaction.
///
/// # Errors
///
/// Returns [`LanguageError::GraphTooLarge`] if the tenant graph exceeds
/// `max_nodes`, [`LanguageError::UnsupportedMutation`] for SQL/PGQ or a
/// statement that changes nothing, [`LanguageError::Storage`] on read failure,
/// and [`LanguageError::Initialization`], [`LanguageError::InvalidParameters`],
/// or [`LanguageError::Query`] if initialization, parameter decoding, or
/// execution fails.
pub fn stage_statement_mutation(
    lmpg: &Lmpg,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_nodes: usize,
) -> Result<rushdb_lmpg::BatchTx, LanguageError> {
    let base = Arc::new(lmpg.store.read_view()?);
    let overlay = Arc::new(OverlayView::new(base));
    stage_statement_mutation_in_overlay(
        lmpg, language, statement, parameters, max_nodes, &overlay, None,
    )
}

/// [`stage_statement_mutation`] over a caller-supplied overlay so a statement
/// sees the transaction's earlier staged writes (cross-statement
/// read-your-writes, T-5b).
///
/// # Errors
/// Returns the same errors as [`stage_statement_mutation`].
pub fn stage_statement_mutation_in_overlay(
    lmpg: &Lmpg,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_nodes: usize,
    overlay: &Arc<OverlayView>,
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<rushdb_lmpg::BatchTx, LanguageError> {
    on_language_stack(statement, &|| false, &overlay.cancellation(), || {
        stage_statement_mutation_in_overlay_on_stack(
            lmpg, language, statement, parameters, max_nodes, overlay, text,
        )
    })
}

/// The body of [`stage_statement_mutation_in_overlay`], on the language stack.
fn stage_statement_mutation_in_overlay_on_stack(
    lmpg: &Lmpg,
    language: QueryLanguage,
    statement: &str,
    parameters: Option<&Json>,
    max_nodes: usize,
    overlay: &Arc<OverlayView>,
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<rushdb_lmpg::BatchTx, LanguageError> {
    if language == QueryLanguage::SqlPgq {
        return Err(LanguageError::UnsupportedMutation(
            "SQL/PGQ is a read-only SQL:2023 GRAPH_TABLE profile".into(),
        ));
    }
    let store = Arc::new(OverlayStore::new(
        LmpgGraphStore::new(Arc::clone(overlay), lmpg.tenant, text.cloned()),
        max_nodes,
        MUTATION_MAX_BYTES,
    ));
    let graph_store: Arc<dyn GraphStoreMut> = store.clone();
    let db = GrafeoDB::with_store(graph_store, Config::in_memory())
        .map_err(|error| LanguageError::Initialization(error.to_string()))?;
    let params = parameters.map(parameters_from_json).transpose()?;
    let session = db.session();
    let language_name = match language {
        QueryLanguage::Gql => "gql",
        QueryLanguage::Cypher => "cypher",
        QueryLanguage::Gremlin => "gremlin",
        QueryLanguage::SqlPgq => unreachable!(),
    };
    let execution = session.execute_language(statement, language_name, params);
    store.check()?;
    execution.map_err(|error| LanguageError::Query {
        language,
        message: error.to_string(),
    })?;
    Ok(store.delta()?.batch)
}

pub(crate) fn all_fields(
    properties: &BTreeMap<PropertyKey, Value>,
) -> Vec<(String, rushdb_lmpg::FieldChange)> {
    properties
        .iter()
        .map(|(key, value)| {
            (
                key.to_string(),
                rushdb_lmpg::FieldChange::Set(value_to_json(value.clone())),
            )
        })
        .collect()
}

pub(crate) fn changed_fields(
    before: &BTreeMap<PropertyKey, Value>,
    after: &BTreeMap<PropertyKey, Value>,
) -> Vec<(String, rushdb_lmpg::FieldChange)> {
    before
        .keys()
        .chain(after.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|key| match (before.get(key), after.get(key)) {
            (left, right) if left == right => None,
            (_, Some(value)) => Some((
                key.to_string(),
                rushdb_lmpg::FieldChange::Set(value_to_json(value.clone())),
            )),
            (Some(_), None) => Some((key.to_string(), rushdb_lmpg::FieldChange::Delete)),
            (None, None) => None,
        })
        .collect()
}

#[expect(
    clippy::too_many_lines,
    reason = "one value-to-JSON mapping over every entity shape Grafeo returns"
)]
fn mutable_result_value_to_json(
    store: &dyn GraphStore,
    node_public_ids: &dyn Fn(NodeId) -> Option<[u8; 16]>,
    edge_public_ids: &dyn Fn(EdgeId) -> Option<[u8; 16]>,
    logical_type: Option<&LogicalType>,
    value: Value,
) -> Json {
    // Grafeo returns created entities as maps in mutation projections even
    // though read projections carry the dedicated Node/Edge logical type.
    // Recognize that canonical entity map and externalize its temporary IDs.
    if let Value::Map(properties) = &value {
        if let Some(Value::Int64(id)) = properties.get(&PropertyKey::new("_id")) {
            if *id >= 0 && properties.contains_key(&PropertyKey::new("_type")) {
                return mutable_result_value_to_json(
                    store,
                    node_public_ids,
                    edge_public_ids,
                    Some(&LogicalType::Edge),
                    Value::Int64(*id),
                );
            }
            if *id >= 0 && properties.contains_key(&PropertyKey::new("_labels")) {
                return mutable_result_value_to_json(
                    store,
                    node_public_ids,
                    edge_public_ids,
                    Some(&LogicalType::Node),
                    Value::Int64(*id),
                );
            }
        }
    }
    match (logical_type, &value) {
        (Some(LogicalType::Node), Value::Int64(id)) if *id >= 0 => store
            .get_node(NodeId::new(u64::try_from(*id).unwrap_or_default()))
            .map_or(Json::Null, |node| {
                let mut object = node
                    .properties
                    .iter()
                    .map(|(key, value)| (key.to_string(), value_to_json(value.clone())))
                    .collect::<JsonMap<_, _>>();
                object.insert(
                    ID_FIELD.into(),
                    node_public_ids(node.id).map_or(Json::Null, |id| {
                        Json::String(uuid::Uuid::from_bytes(id).to_string())
                    }),
                );
                object.insert(
                    LABEL_FIELD.into(),
                    node.labels
                        .first()
                        .map_or(Json::Null, |label| Json::String(label.to_string())),
                );
                Json::Object(object)
            }),
        (Some(LogicalType::Edge), Value::Int64(id)) if *id >= 0 => store
            .get_edge(EdgeId::new(u64::try_from(*id).unwrap_or_default()))
            .map_or(Json::Null, |edge| {
                let mut object = edge
                    .properties
                    .iter()
                    .map(|(key, value)| (key.to_string(), value_to_json(value.clone())))
                    .collect::<JsonMap<_, _>>();
                object.insert(
                    ID_FIELD.into(),
                    edge_public_ids(edge.id).map_or(Json::Null, |id| {
                        Json::String(uuid::Uuid::from_bytes(id).to_string())
                    }),
                );
                object.insert(TYPE_FIELD.into(), Json::String(edge.edge_type.to_string()));
                object.insert(
                    SOURCE_FIELD.into(),
                    node_public_ids(edge.src).map_or(Json::Null, |id| {
                        Json::String(uuid::Uuid::from_bytes(id).to_string())
                    }),
                );
                object.insert(
                    TARGET_FIELD.into(),
                    node_public_ids(edge.dst).map_or(Json::Null, |id| {
                        Json::String(uuid::Uuid::from_bytes(id).to_string())
                    }),
                );
                Json::Object(object)
            }),
        (_, Value::Path { nodes, edges }) => {
            let entities = |values: &[Value], kind: LogicalType| -> Vec<Json> {
                values
                    .iter()
                    .map(|value| {
                        mutable_result_value_to_json(
                            store,
                            node_public_ids,
                            edge_public_ids,
                            Some(&kind),

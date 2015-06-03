//! Transport-neutral, versioned `RushDB` database protocol.
//!
//! These types describe database capabilities. HTTP, gRPC/Connect, embedded
//! callers, and application adapters are transports around this contract;
//! product resources and SDK response shapes do not belong here.

use serde::{Deserialize, Serialize};

/// Wire protocol version spoken by these envelopes.
///
/// Bumped whenever the wire shape changes in a backward-incompatible way.
pub const PROTOCOL_VERSION: u16 = 1;

/// Constant-time comparison of two secrets (bearer tokens, passwords).
///
/// Returns `false` for differing lengths (a token's length is not itself a
/// secret). Use this instead of `==` for authentication so the comparison time
/// does not reveal how many leading bytes matched.
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    // Defeat an early-exit rewrite of the accumulate loop.
    std::hint::black_box(diff) == 0
}

/// Versioned request envelope wrapping a single [`Operation`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestEnvelope {
    /// Protocol version the caller speaks; must equal [`PROTOCOL_VERSION`].
    pub protocol_version: u16,
    /// Optional caller-chosen id echoed back on the response for correlation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Request-scoped context (tenancy).
    #[serde(default)]
    pub context: RequestContext,
    /// The operation to perform.
    pub operation: Operation,
}

/// Request-scoped context carried alongside every operation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestContext {
    /// Opaque tenancy value supplied by the trusted host. The configured
    /// discriminator property remains server-side configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Optional execution deadline requested by the trusted host, in
    /// milliseconds from request admission. The engine treats this as a
    /// work deadline, not an identity or authorization decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    /// Optional working-set cap of this request's queries, in bytes; it
    /// may only tighten the server's `query-memory-max`. Must be
    /// positive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_memory_max_bytes: Option<u64>,
    /// Optional limit on the records a filter without a usable index may
    /// check; `0` refuses every such walk. It may only tighten the server's
    /// `unindexed-walk-max-owners`, like `CockroachDB`'s
    /// `disallow_full_table_scans` with `large_full_scan_rows`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unindexed_walk_max_owners: Option<u64>,
}

/// A single protocol operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Operation {
    /// Execute a read-only query.
    Query {
        /// The query to execute.
        query: Query,
    },
    /// Execute a mutation.
    Mutate {
        /// The mutation to apply.
        mutation: Mutation,
    },
    /// Introspect schema, capabilities, or query plans.
    Describe {
        /// The description request.
        describe: Describe,
    },
    /// Ingest documents.
    Ingest {
        /// The ingest request.
        ingest: Ingest,
    },
    /// Manage vector indexes.
    Index {
        /// The index operation.
        index: IndexOperation,
    },
    /// Manage a multi-statement transaction.
    Transaction {
        /// The transaction operation.
        transaction: TransactionOperation,
    },
    /// Drop every tenant-scoped key for the request scope. Used by
    /// application-level project deletion so a deleted project's data cannot
    /// leak into a later reuse of the same scope value.
    PurgeTenant,
    /// Consistent logical backup. The graph is pinned at a commit index and
    /// exported as JSONL while the server keeps serving writes. On the public
    /// multitenant protocol the scopes must match the authenticated request
    /// scope; an empty list means that one scope. Fleet-wide export is an
    /// offline/operator operation.
    Backup {
        /// Backup selection parameters.
        request: BackupRequest,
    },
    /// Read or replace the request tenant's data-access policy. Operator
    /// only: over HTTP it needs the boot token, so a capability token cannot
    /// change the policy that limits it.
    Policy {
        /// The policy operation.
        policy: PolicyOperation,
    },
}

/// A data-access policy operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum PolicyOperation {
    /// The tenant's active policy, if any.
    Get,
    /// Install a policy as the tenant's next generation. A transaction begun
    /// under the previous generation aborts at commit.
    Put {
        /// `allowedLabels`, `deniedRelTypes`, `visibleProperties`
        /// (label → the only properties a reader sees) and `maxResults`.
        policy: serde_json::Value,
        /// Whether reads and writes are filtered by the policy; `false`
        /// keeps tenant isolation only.
        enforced: bool,
    },
}

/// Parameters for a logical backup.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupRequest {
    /// Scopes to include. In multitenant protocol mode every value must equal
    /// the authenticated request scope; empty means that one scope.
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// Value-domain mode for property-value queries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PropertyValuesMode {
    /// Full distinct list (legacy behavior; `limit` may truncate output).
    #[default]
    Values,
    /// Exact distinct count only (`values` empty, `distinct_count` set).
    Count,
    /// Min/max only (`values` empty, `min`/`max` set).
    Range,
    /// Bounded most-frequent values with owner counts (`counts` aligned with
    /// `values`); exact, answered from the equality postings when available.
    TopK,
}

/// Text filter over the distinct values of a property-value query:
/// keeps the values whose text matches. A string matches as stored; a number,
/// boolean or datetime matches as the response renders it; an array matches
/// when any element does. A text that reads as a date (year first, at least a
/// month: `1993-11-12`, `1993/11/12`, `2010/2/22`, `2013-06`) also matches
/// values holding that date in another layout (`1993-11-12T00:00:00Z`,
/// `1993.11.12`, `19931112`, `201306`), at the text's precision. The
/// engine matches with `rushdb_types::value_match::ValueMatcher`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValueSearch {
    /// The text to look for.
    pub text: String,
    /// How `text` must match a value (default `contains`).
    #[serde(default, rename = "match")]
    pub matching: ValueMatch,
    /// Compare case-sensitively (default: case-insensitive).
    #[serde(default)]
    pub case_sensitive: bool,
    /// When no value matches and the property has at most this many distinct
    /// values in scope, answer with all of them (by descending owner count,
    /// with counts) and set `searchMissed`, so a caller sees how the values
    /// are actually written instead of an empty result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_on_miss: Option<u64>,
    /// `similar` only: the least similarity a value needs, from 0 to 1
    /// (default 0.6). See [`ValueMatch::Similar`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_similarity: Option<f64>,
}

/// How [`ValueSearch::text`] must match a value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ValueMatch {
    /// The value contains the text.
    #[default]
    Contains,
    /// The value starts with the text.
    Prefix,
    /// The value equals the text.
    Exact,
    /// The value is spelled like the text: equal once case, punctuation and
    /// spacing are ignored, or sharing enough of the text's character
    /// trigrams (`minSimilarity`). Values rank by similarity, then by owner
    /// count. These are near matches, not exact ones.
    Similar,
}

/// A read-only query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Query {
    /// A Gremlin traversal as `TinkerPop` `GraphSON` 3 bytecode (what language
    /// variants send), run as the equivalent Gremlin text traversal.
    GremlinBytecode {
        /// The `g:Bytecode` value (or its untyped `{"step": [...]}` form).
        bytecode: serde_json::Value,
        /// Optional bind parameters.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parameters: Option<serde_json::Value>,
    },
    /// Textual query-language frontend. Every language is transported by the
    /// same operation and must lower into an engine-owned logical plan.
    Statement {
        /// The language the statement is written in.
        language: StatementLanguage,
        /// The statement text.
        statement: String,
        /// Optional bind parameters for the statement.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parameters: Option<serde_json::Value>,
    },
    /// Native structured query.
    Records {
        /// The native query document.
        query: QueryDocument,
    },
    /// Continue a tenant-bound, snapshot-pinned native records cursor.
    /// Cursors are deliberately a Protocol v1 feature; compatibility query
    /// languages retain their ordinary materialized response contracts.
    Cursor {
        /// Cursor token returned by a previous records response.
        cursor_id: String,
        /// Maximum number of records to return in this page.
        limit: u64,
    },
    /// Fetch a single record by id.
    Record {
        /// The record id.
        id: String,
    },
    /// Query relationships.
    Relationships {
        /// The relationship query.
        query: RelationshipQuery,
    },
    /// Distinct property values.
    PropertyValues {
        /// The property to collect values for.
        property: String,
        /// The query constraining the records considered.
        query: QueryDocument,
        /// Value-domain mode. `None` = `Values` (legacy full distinct
        /// list). `Count` returns only the exact distinct count; `Range`
        /// returns only min/max; `TopK` returns the `limit` most frequent
        /// values (exact).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mode: Option<PropertyValuesMode>,
        /// Bounds shipped `values` in `Values` mode (by descending record
        /// count, ties by value).
        /// `distinct_count` always reports the untruncated total and
        /// `truncated` flags truncation. Ignored by `Count`/`Range`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Keep only the distinct values whose text matches. Every
        /// mode then answers over the matching values: `distinct_count`
        /// counts matches, and `Values` lists them by descending owner count
        /// (ties by value), answered from the value dictionaries when they
        /// cover the property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        search: Option<ValueSearch>,
    },
    /// Vector similarity query.
    Vector {
        /// The vector query.
        query: VectorQuery,
    },
    /// Pairwise join of two record sets.
    JoinPairs(JoinPairsQuery),
    /// Records of a label+property that are not yet bound to a vector index.
    /// The control plane uses this to drive embedding backfill: fetch a batch
    /// of unbound records, embed their text, then `upsertVectors`.
    VectorIndexCandidates {
        /// The candidate query.
        query: VectorIndexCandidatesQuery,
    },
    /// Lexical (full-text) top-k search against a declared text index.
    Text {
        /// The text index name.
        index: String,
        /// The query string.
        query: String,
        /// Maximum number of hits to return.
        #[serde(default = "default_text_limit")]
        limit: u64,
        /// Every term must match (AND); by default any term may (OR).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        all_terms: bool,
        /// Match the whole query as one phrase (terms adjacent, in order).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        phrase: bool,
    },
    /// Combined graph + vector + lexical query, fused by reciprocal-rank
    /// fusion over one shared, predicate-resolved candidate scope.
    Hybrid {
        /// The hybrid query.
        query: HybridQuery,
    },
    /// The records a native query selects, rendered as CSV text: an `__id`
    /// column, an `__label` column, then every property name in sorted order.
    /// `select` and `groupBy` are rejected (the export is of records).
    ExportCsv {
        /// The native query document.
        query: QueryDocument,
    },
    /// Value search across every property, without naming one:
    /// the stored values that match `search`, with the label and property
    /// that hold them and their owner counts, by descending count. `labels`
    /// narrows the labels searched (all when empty).
    SearchValues {
        /// The search (`domainOnMiss` is ignored here).
        search: ValueSearch,
        /// Labels to search (all labels when empty).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        labels: Vec<String>,
        /// Most matches returned (default 50, at most 1000); `total` counts
        /// them all.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
    },
}

fn default_text_limit() -> u64 {
    10
}

/// Query for records not yet bound to a vector index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexCandidatesQuery {
    /// Label whose records are scanned.
    pub label: String,
    /// Property whose vector index bindings are checked.
    pub property: String,
    /// The index id (the canonical `emb__<label>__<property>__<sim>__<dims>`
    /// alias) to check bindings against.
    pub index: String,
    /// Number of matching records to skip.
    #[serde(default)]
    pub skip: u64,
    /// Maximum number of records to return.
    #[serde(default = "default_candidate_limit")]
    pub limit: u64,
}

fn default_candidate_limit() -> u64 {
    500
}

/// A textual query language frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StatementLanguage {
    /// `RushDB`'s relational SQL frontend (also used by pgwire).
    Sql,
    /// ISO/IEC 39075 Graph Query Language. This is not GraphQL.
    Gql,
    /// openCypher-compatible graph query language.
    Cypher,
    /// Apache `TinkerPop` Gremlin text traversal language.
    Gremlin,
    /// SQL:2023 property graph queries using `GRAPH_TABLE`.
    SqlPgq,
    /// MongoDB-style query documents: find/aggregate + insert/update/delete
    /// commands, translated onto the native query/mutate surface.
    Mongo,
}

impl StatementLanguage {
    /// Machine-readable language id (matches `describe.capabilities`).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            StatementLanguage::Sql => "sql",
            StatementLanguage::Gql => "gql",
            StatementLanguage::Cypher => "cypher",
            StatementLanguage::Gremlin => "gremlin",
            StatementLanguage::SqlPgq => "sqlPgq",
            StatementLanguage::Mongo => "mongo",
        }
    }
}

/// Startup language enablement. `searchquery` (the native `QueryDocument`) is
/// NOT listed here: it is always enabled and cannot be disabled. The default
/// enables the stable languages (`sql`, `gremlin`, `sql-pgq`, `mongodb`);
/// the release-candidate `cypher` and `gql` are opt-in. `config.yml`
/// (`languages:`) or `--languages` replaces the whole set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Language enablement is a fixed wire contract of independent opt-in flags; \
              collapsing them into enums would change the serialized `LanguageSet` shape"
)]
pub struct LanguageSet {
    /// `DataFusion` relational SQL + PostgreSQL wire protocol.
    pub sql: bool,
    /// ISO GQL.
    pub gql: bool,
    /// openCypher.
    pub cypher: bool,
    /// Apache `TinkerPop` Gremlin.
    pub gremlin: bool,
    /// SQL:2023 `GRAPH_TABLE` (read-only).
    pub sql_pgq: bool,
    /// MongoDB query language.
    pub mongodb: bool,
}

impl Default for LanguageSet {
    fn default() -> Self {
        Self {
            sql: true,
            gremlin: true,
            sql_pgq: true,
            mongodb: true,
            ..Self::NONE
        }
    }
}

impl LanguageSet {
    /// No statement language: only the native query document.
    pub const NONE: Self = Self {
        sql: false,
        gql: false,
        cypher: false,
        gremlin: false,
        sql_pgq: false,
        mongodb: false,
    };

    /// Parse a comma-separated `--languages` value. `searchquery` is implicit
    /// and rejected if explicitly listed as a removable language.
    ///
    /// # Errors
    ///
    /// Returns an error if the list explicitly names the always-enabled
    /// `searchquery` language or contains an unknown language name.
    pub fn from_list(s: &str) -> Result<Self, String> {
        let mut set = LanguageSet::NONE;
        for raw in s.split(',') {
            let name = raw.trim();
            if name.is_empty() {
                continue;
            }
            match name {
                "searchquery" => {
                    return Err("`searchquery` is always enabled and cannot be disabled".into())
                }
                "sql" => set.sql = true,
                "gql" => set.gql = true,
                "cypher" => set.cypher = true,
                "gremlin" => set.gremlin = true,
                "sql-pgq" => set.sql_pgq = true,
                "mongodb" => set.mongodb = true,
                other => return Err(format!("unknown language `{other}`")),
            }
        }
        Ok(set)
    }

    /// Whether a statement language is enabled.
    #[must_use]
    pub fn enabled(&self, language: StatementLanguage) -> bool {
        match language {
            StatementLanguage::Sql => self.sql,
            StatementLanguage::Gql => self.gql,
            StatementLanguage::Cypher => self.cypher,
            StatementLanguage::Gremlin => self.gremlin,
            StatementLanguage::SqlPgq => self.sql_pgq,
            StatementLanguage::Mongo => self.mongodb,
        }
    }

    /// The enabled statement languages, for capability advertisement.
    #[must_use]
    pub fn advertised(&self) -> Vec<StatementLanguage> {
        [
            (StatementLanguage::Sql, self.sql),
            (StatementLanguage::Gql, self.gql),
            (StatementLanguage::Cypher, self.cypher),
            (StatementLanguage::Gremlin, self.gremlin),
            (StatementLanguage::SqlPgq, self.sql_pgq),
            (StatementLanguage::Mongo, self.mongodb),
        ]
        .into_iter()
        .filter(|(_, on)| *on)
        .map(|(language, _)| language)
        .collect()
    }
}

/// `RushDB`'s structured query document. Its predicate/projection grammar is a
/// database concern and is parsed by `rushdb-query`. The document is an exact
/// contract: an unknown key is rejected, never skipped, so a typo
/// cannot silently widen a query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryDocument {
    /// Labels the records must carry (empty = any label).
    #[serde(default)]
    pub labels: Vec<String>,
    /// Predicate document filtering candidate records.
    #[serde(default = "empty_object")]
    pub predicate: serde_json::Value,
    /// Output-shaping select expression map (`select` in the SDK/API surface).
    /// The legacy top-level `aggregate` clause is extinct: `select` + `groupBy`
    /// is the canonical shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub select: Option<serde_json::Value>,
    /// Group-key field names (`groupBy` in the SDK/API surface).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// Sort expression document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<serde_json::Value>,
    /// Number of matching records to skip.
    #[serde(default)]
    pub offset: u64,
    /// Maximum number of records to return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// Start a resumable native records cursor. A finite `limit` is required.
    /// The server caps the requested lease and binds the token to this tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_ttl_ms: Option<u64>,
}

impl Default for QueryDocument {
    fn default() -> Self {
        Self {
            labels: Vec::new(),
            predicate: empty_object(),
            select: None,
            group_by: Vec::new(),
            order: None,
            offset: 0,
            limit: None,
            cursor_ttl_ms: None,
        }
    }
}

/// Query over relationships between records.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipQuery {
    /// Labels the relationship source must carry.
    #[serde(default)]
    pub source_labels: Vec<String>,
    /// Labels the relationship target must carry.
    #[serde(default)]
    pub target_labels: Vec<String>,
    /// Restrict to this relationship type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation_type: Option<String>,
    /// Predicate document filtering relationships.
    #[serde(default = "empty_object")]
    pub predicate: serde_json::Value,
    /// Record predicate the source must satisfy (`query.records` syntax,
    /// `$id` included). Resolved through the source records' own links, so
    /// the cost follows their relationship count, not the store's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_predicate: Option<serde_json::Value>,
    /// Record predicate the target must satisfy; see `source_predicate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_predicate: Option<serde_json::Value>,
    /// Number of matching relationships to skip.
    #[serde(default)]
    pub offset: u64,
    /// Maximum number of relationships to return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

/// A write operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Mutation {
    /// A Gremlin write traversal as `GraphSON` 3 bytecode; see
    /// [`Query::GremlinBytecode`].
    GremlinBytecode {
        /// The `g:Bytecode` value.
        bytecode: serde_json::Value,
        /// Optional bind parameters.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parameters: Option<serde_json::Value>,
    },
    /// Graph-language write. Keeping this under `Mutation` makes authorization
    /// independent of statement text and prevents a write from entering via a
    /// read-authorized `Query::Statement` operation.
    Statement {
        /// The language the statement is written in.
        language: StatementLanguage,
        /// The statement text.
        statement: String,
        /// Optional bind parameters for the statement.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parameters: Option<serde_json::Value>,
    },
    /// Create, merge, or replace a single record.
    PutRecord {
        /// How to reconcile with an existing record.
        mode: PutMode,
        /// Optional explicit record id.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        /// Optional primary label.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        /// Record properties.
        properties: serde_json::Value,
        /// Optional vectors to associate with the record.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        vectors: Vec<serde_json::Value>,
        /// Put options document.
        #[serde(default = "empty_object")]
        options: serde_json::Value,
    },
    /// Delete a record by id.
    DeleteRecord {
        /// The record id.
        id: String,
    },
    /// Atomically change a record's single primary label. The stable record
    /// id, ordinal, fields, vectors, and relationships are retained.
    RetypeRecord {
        /// The record id.
        id: String,
        /// The current primary label.
        from_label: String,
        /// The new primary label.
        to_label: String,
    },
    /// Delete every record matching a query.
    DeleteRecords {
        /// The query selecting records to delete.
        query: QueryDocument,
    },
    /// Remove a property (field) from the tenant: every occurrence on records
    /// and relationships, plus the family and definitions. `name` is matched
    /// against the normalized property name.
    DeleteProperty {
        /// The normalized property name to delete.
        name: String,
    },
    /// Link records matched by label, key, and predicate.
    LinkRecords {
        /// The relationship mutation.
        command: RelationshipMutation,
    },
    /// Remove links matched by label, key, and predicate.
    UnlinkRecords {
        /// The relationship mutation.
        command: RelationshipMutation,
    },
    /// Retype relationships between two labels.
    RetypeRelations {
        /// The retype mutation.
        command: RetypeRelationsMutation,
    },
    /// Link records by explicit record IDs (attach semantics). Unlike
    /// `LinkRecords` (label + key + predicate join), the endpoints are named
    /// directly, so Core's attach/detach-by-id surface maps without a label
    /// join. Idempotent per pair (MERGE semantics).
    LinkRecordsById {
        /// The by-id relationship mutation.
        command: RelationshipIdsMutation,
    },
    /// Delete the relationships between explicit source and target records.
    UnlinkRecordsById {
        /// Which relationships to delete.
        command: UnlinkRecordsMutation,
    },
    /// Pairwise batch link: explicit (source, target) pairs, one edge per
    /// pair — unlike `LinkRecordsById`, which is cross-product. The whole
    /// batch resolves up front (dangling id fails the call) and commits
    /// atomically in one batch; existing edges are skipped and reported.
    /// Per-pair `relationType` overrides the top-level default, so one call
    /// can carry heterogeneous edge types.
    LinkRecordsPairs {
        /// The pairwise relationship mutation.
        command: RelationshipPairsMutation,
    },
}

/// Rename the relationship type between two labels: every existing edge of
/// `source_type` whose endpoints are a `source_label` record and a
/// `target_label` record is re-created as `target_type` (endpoints preserved,
/// direction honored), then the old edge is removed. Mirrors the Neo4j
/// retype semantics used by relationship-pattern approval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetypeRelationsMutation {
    /// Label the source endpoint must carry.
    pub source_label: String,
    /// Label the target endpoint must carry.
    pub target_label: String,
    /// Relationship type to rewrite from.
    #[serde(default = "default_rel_type")]
    pub source_type: String,
    /// Relationship type to rewrite to.
    pub target_type: String,
    /// Direction of the relationship.
    #[serde(default)]
    pub direction: Direction,
}

fn default_rel_type() -> String {
    "__RUSHDB__RELATION__DEFAULT__".to_string()
}

/// Record write reconciliation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PutMode {
    /// Fail if a record already exists.
    Create,
    /// Update the record if it exists, otherwise create it.
    Merge,
    /// Overwrite the record entirely.
    Replace,
}

/// Link records selected by label/key/predicate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipMutation {
    /// The relationship type to create.
    pub relation_type: String,
    /// Selector for the source endpoint.
    pub source: RelationshipSelector,
    /// Selector for the target endpoint.
    pub target: RelationshipSelector,
    /// Direction of the relationship.
    #[serde(default)]
    pub direction: Direction,
    /// Allow a cross-product when a selector matches multiple records.
    #[serde(default)]
    pub allow_cartesian: bool,
    /// Relationship properties.
    #[serde(default = "empty_object")]
    pub properties: serde_json::Value,
}

/// Selects records of a label matching a predicate and optional property key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipSelector {
    /// Label the selected records must carry.
    pub label: String,
    /// Optional property key used to identify records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    /// Predicate document filtering records.
    #[serde(default = "empty_object")]
    pub predicate: serde_json::Value,
}

/// By-ID relationship mutation (attach/detach). `source_ids` and `target_ids`
/// are record `_id` values (dashed UUID or compact hex). For each source→target
/// pair the engine creates/deletes a relationship of `relation_type`.
/// `direction: in` treats the pair as target→source (endpoint swap).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipIdsMutation {
    /// The relationship type to create or delete.
    pub relation_type: String,
    /// Direction of the relationship.
    #[serde(default)]
    pub direction: Direction,
    /// Source endpoint record ids.
    pub source_ids: Vec<String>,
    /// Target endpoint record ids.
    pub target_ids: Vec<String>,
    /// Relationship properties.
    #[serde(default = "empty_object")]
    pub properties: serde_json::Value,
}

/// Delete the relationships between every source and every target record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnlinkRecordsMutation {
    /// Relationship types to delete; empty deletes relationships of every
    /// type.
    #[serde(default)]
    pub relation_types: Vec<String>,
    /// Direction from source to target; absent deletes relationships in both
    /// directions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<Direction>,
    /// Source endpoint record ids.
    pub source_ids: Vec<String>,
    /// Target endpoint record ids.
    pub target_ids: Vec<String>,
}

/// One explicit edge of a pairwise batch link.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipPair {
    /// Source endpoint record id.
    pub source_id: String,
    /// Target endpoint record id.
    pub target_id: String,
    /// Per-pair edge type; absent = the batch `relation_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation_type: Option<String>,
}

/// Pairwise batch link: N explicit pairs → up to N edges in one atomic
/// commit (no cross-product expansion).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipPairsMutation {
    /// Default relationship type applied to pairs without their own.
    pub relation_type: String,
    /// Direction of the relationships.
    #[serde(default)]
    pub direction: Direction,
    /// Explicit source/target pairs.
    pub pairs: Vec<RelationshipPair>,
    /// Relationship properties.
    #[serde(default = "empty_object")]
    pub properties: serde_json::Value,
}

/// Direction of a relationship.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Direction {
    /// Source → target.
    #[default]
    Out,
    /// Target → source.
    In,
}

/// An introspection request. Unknown fields are rejected, as in a query
/// document: a `schemaSummary` sent a `query` predicate used to ignore it and
/// summarize the whole tenant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Describe {
    /// Describe schema for the records matching a query.
    Schema {
        /// The query constraining the described records.
        query: QueryDocument,
        /// Property-entry detail level. `None`/`Full` reads up to
        /// 10k records for samples/min/max; `Catalog` answers names, types,
        /// exact support counts, array shape, and sibling flags from catalog
        /// indexes alone (no record scan).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<PropertyDetail>,
        /// Property-entry pagination: slice of the `properties` list.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        skip: Option<u64>,
        /// Maximum number of property entries to return.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
    },
    /// List labels.
    Labels {
        /// The query constraining the described labels.
        query: QueryDocument,
    },
    /// Describe properties.
    Properties {
        /// The query constraining the described properties.
        query: QueryDocument,
        /// Entry detail level: `None`/`Full` reads up to 10k
        /// records for samples/min/max; `Catalog` answers names, types,
        /// exact support counts, array shape, and sibling flags from catalog
        /// indexes alone (no record scan) and rejects a non-empty `where`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<PropertyDetail>,
        /// Property-entry pagination: slice of the returned entry list.
        /// Independent of the stats scan window.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        skip: Option<u64>,
        /// Maximum number of property entries to return.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
    },
    /// Machine-readable statement-language profiles. Clients must use this
    /// instead of inferring compatibility from a server version string.
    QueryLanguages,
    /// Feature-capability report (see [`Response::Capabilities`]).
    Capabilities,
    /// Version/build identity, transport bindings, and configured limits
    /// (see [`Response::BuildInfo`]).
    BuildInfo,
    /// Query plan introspection for a `QueryDocument` (see
    /// [`Response::Explain`]).
    Explain {
        /// The query document to explain.
        query: QueryDocument,
    },
    /// Query plan introspection for a vector query (see
    /// [`Response::ExplainVector`]): access path, candidate count, top-k.
    ExplainVector {
        /// The vector query to explain.
        query: VectorQuery,
    },
    /// List declared (non-vector) indexes with their state.
    Indexes,
    /// List vector indexes.
    VectorIndexes,
    /// Describe a single vector index by name.
    VectorIndex {
        /// The vector index name.
        name: String,
    },
    /// Labels and properties whose names are spelled like `text`: the
    /// answer to "no such table/column". Ignores case, separators
    /// and plural endings, then ranks by spelling similarity.
    Names {
        /// The name to resolve, e.g. `patients` or `Patient.birth_date`.
        text: String,
        /// Most suggestions (default 5).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
    },
    /// Every label with its properties in one compact read: types,
    /// exact support and distinct counts, numeric/date ranges, the values of
    /// properties holding few, and relationship shapes.
    SchemaSummary {
        /// Labels to summarize (all when empty).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        labels: Vec<String>,
        /// List a property's values when it holds at most this many
        /// distinct ones (default 10, at most 100; 0 lists none).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        few_values: Option<u64>,
    },
    /// Value overlap between property pairs: for each pair, how
    /// many of the source's distinct values the target also holds, over the
    /// exact value dictionaries. Evidence only: deciding which pairs are
    /// references is the caller's (the Core platform's suggestions).
    ValueOverlap {
        /// The pairs to compare (at most 10,000).
        pairs: Vec<JoinPair>,
    },
}

/// One side of a join: a label and one of its properties.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinEnd {
    /// The label.
    pub label: String,
    /// The property.
    pub property: String,
}

/// A pair of properties to compare: does the source's values name records
/// of the target by the target property?
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinPair {
    /// The referencing side.
    pub source: JoinEnd,
    /// The referenced side (usually a key).
    pub target: JoinEnd,
}

/// Property-entry detail level for `Describe::Properties`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PropertyDetail {
    /// Full statistics: samples, min/max, and array shape. Unfiltered reads
    /// sample up to 10k records and combine that sample with exact catalog
    /// counts; filtered reads scan the complete match set within the query
    /// resource limit so support counts are never silently truncated.
    #[default]
    Full,
    /// Catalog facts only (names, types, exact support counts, array shape,
    /// sibling flags) with no record scan; rejects a non-empty `where`.
    Catalog,
}

/// A document ingest request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Ingest {
    /// Ingest JSON documents.
    Documents {
        /// Explicit label for a top-level record or array. May be omitted for
        /// a non-empty container object whose values are all nested objects
        /// or arrays of nested objects; those top-level keys become labels.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        /// The document or documents to ingest.
        documents: serde_json::Value,
        /// Ingest options document.
        #[serde(default = "empty_object")]
        options: serde_json::Value,
    },
    /// Ingest CSV text: every data row becomes one record of `label`. Rows
    /// are decoded as Papa Parse 5 decodes them, header names are made into
    /// property names, then the rows are ingested as documents.
    Csv {
        /// The label of every imported record.
        label: String,
        /// The CSV text.
        data: String,
        /// How the text is decoded.
        #[serde(default)]
        parse_config: CsvParseConfig,
        /// Ingest options document, as for documents.
        #[serde(default = "empty_object")]
        options: serde_json::Value,
        /// Per-row inline vectors: entry `i` becomes data row `i`'s `$vectors`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        vectors: Vec<serde_json::Value>,
    },
}

/// How CSV text is decoded (`parseConfig` in the SDK/API surface). Every
/// field is optional; the defaults read a header row, split on `,` and skip
/// empty lines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CsvParseConfig {
    /// Field separator; any non-empty string.
    #[serde(default = "default_csv_delimiter")]
    pub delimiter: String,
    /// The first row names the fields. Without it, fields are named
    /// `column_1`, `column_2`, ...
    #[serde(default = "default_true")]
    pub header: bool,
    /// Which lines are skipped as empty.
    #[serde(default)]
    pub skip_empty_lines: CsvSkipEmptyLines,
    /// Turn numbers, `true`/`false` and ISO datetimes into typed values.
    /// Defaults to the `suggestTypes` import option.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic_typing: Option<bool>,
    /// The quote character.
    #[serde(default = "default_csv_quote")]
    pub quote_char: String,
    /// The character that escapes a quote inside a quoted field; defaults to
    /// the quote character (a doubled quote).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escape_char: Option<String>,
    /// Line ending: `\n`, `\r` or `\r\n`. Any other value, or none, is
    /// detected from the text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newline: Option<String>,
}

impl Default for CsvParseConfig {
    fn default() -> Self {
        CsvParseConfig {
            delimiter: default_csv_delimiter(),
            header: true,
            skip_empty_lines: CsvSkipEmptyLines::default(),
            dynamic_typing: None,
            quote_char: default_csv_quote(),
            escape_char: None,
            newline: None,
        }
    }
}

fn default_csv_delimiter() -> String {
    ",".to_string()
}

fn default_csv_quote() -> String {
    "\"".to_string()
}

fn default_true() -> bool {
    true
}

/// Which CSV lines are skipped: `true` skips lines with no characters,
/// `"greedy"` also skips lines whose fields are all whitespace, `false` keeps
/// every line.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CsvSkipEmptyLines {
    /// Keep every line.
    Keep,
    /// Skip lines with no characters.
    #[default]
    Empty,
    /// Skip lines whose fields are all whitespace.
    Greedy,
}

impl Serialize for CsvSkipEmptyLines {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            CsvSkipEmptyLines::Keep => serializer.serialize_bool(false),
            CsvSkipEmptyLines::Empty => serializer.serialize_bool(true),
            CsvSkipEmptyLines::Greedy => serializer.serialize_str("greedy"),
        }
    }
}

impl<'de> Deserialize<'de> for CsvSkipEmptyLines {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::Bool(true) => Ok(CsvSkipEmptyLines::Empty),
            serde_json::Value::Bool(false) => Ok(CsvSkipEmptyLines::Keep),
            serde_json::Value::String(value) if value == "greedy" => Ok(CsvSkipEmptyLines::Greedy),
            _ => Err(serde::de::Error::custom(
                "skipEmptyLines must be true, false or \"greedy\"",
            )),
        }
    }
}

/// A vector index management operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum IndexOperation {
    /// Declare an ordered or text index on a label's property. It is
    /// built online; `describe.indexes` reports `building` until it is
    /// `ready`.
    Create {
        /// The index definition.
        definition: IndexDefinition,
    },
    /// Drop a declared index by name (removed online).
    Drop {
        /// The index name.
        name: String,
    },
    /// Create a vector index.
    CreateVector {
        /// The index definition.
        definition: VectorIndexDefinition,
    },
    /// Drop a vector index by name.
    DropVector {
        /// The index name.
        name: String,
    },
    /// Insert or update vectors in an index.
    UpsertVectors {
        /// The index name.
        name: String,
        /// The vectors to upsert.
        items: Vec<VectorItem>,
    },
    /// Reconcile a managed vector index's binding statuses against live data
    /// (deleted/updated sources become Retired/Stale), so a stale `ready`
    /// index reports accurate counts and can be re-embedded. For managed
    /// (occurrence-backed) indexes only; external indexes are skipped.
    RevalidateVector {
        /// The index name.
        name: String,
    },
}

/// A transaction management operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TransactionOperation {
    /// Begin a transaction.
    Begin {
        /// Optional per-transaction TTL override (milliseconds). Absent uses
        /// the server default; either value is capped by the server maximum.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
        /// When true, an expired transaction is COMMITTED (best effort) instead
        /// of rolled back when its TTL lapses.
        #[serde(default)]
        autocommit: bool,
    },
    /// Stage a mutation in the transaction.
    Stage {
        /// The transaction id.
        transaction_id: String,
        /// The mutation to stage.
        mutation: TransactionMutation,
    },
    /// Commit the transaction.
    Commit {
        /// The transaction id.
        transaction_id: String,
    },
    /// Roll back the transaction.
    Rollback {
        /// The transaction id.
        transaction_id: String,
    },
}

/// A mutation staged inside a transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TransactionMutation {
    /// Create a record.
    CreateRecord {
        /// The record label.
        label: String,
        /// Optional pinned record id (dashed UUID or compact hex) so other
        /// services can reference the record before commit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        /// Record properties.
        #[serde(default = "empty_object")]
        properties: serde_json::Value,
        /// Value typing options, as for `putRecord` (`suggestTypes`,
        /// `convertNumericValuesToNumbers`, `skipEmptyValues`, ...).
        #[serde(default = "empty_object")]
        options: serde_json::Value,
    },
    /// Update an existing record.
    UpdateRecord {
        /// Reference to the record to update.
        record: RecordReference,
        /// Properties to apply; values are typed as `putRecord` merge and
        /// replace type them.
        #[serde(default = "empty_object")]
        properties: serde_json::Value,
        /// `true` also deletes every property `properties` does not name, as
        /// `putRecord` mode `replace` does; `false` (default) merges.
        #[serde(default)]
        replace: bool,
    },
    /// Transactional counterpart to the native `retypeRecord` mutation.
    RetypeRecord {
        /// Reference to the record to retype.
        record: RecordReference,
        /// The current primary label.
        from_label: String,
        /// The new primary label.
        to_label: String,
    },
    /// Delete a record.
    DeleteRecord {
        /// Reference to the record to delete.
        record: RecordReference,
    },
    /// Create a relationship between two records.
    CreateRelationship {
        /// The relationship type.
        relation_type: String,
        /// The source endpoint.
        source: RecordReference,
        /// The target endpoint.
        target: RecordReference,
        /// Relationship properties.
        #[serde(default = "empty_object")]
        properties: serde_json::Value,
    },
    /// Stage an import (JSON documents or CSV) as plain record creates and
    /// the relationships between them. Merge imports (`mergeBy`,
    /// `mergeStrategy`) and inline vectors are not staged.
    Ingest {
        /// The import, as for the `ingest` operation.
        ingest: Box<Ingest>,
    },
    /// Read one record inside the transaction: the result merges committed
    /// state with the transaction's staged writes (read-your-writes) and the
    /// read's key footprint is recorded so `commit` detects concurrent changes.
    GetRecord {
        /// Reference to the record to read.
        record: RecordReference,
    },
    /// Stage a graph-language mutation (GQL/Cypher/Gremlin) into the
    /// transaction. The statement runs against the committed graph, its exact
    /// delta is staged, and it commits or rolls back with the transaction.
    Statement {
        /// The language the statement is written in.
        language: StatementLanguage,
        /// The statement text.
        statement: String,
        /// Optional bind parameters for the statement.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parameters: Option<serde_json::Value>,
    },
}

/// A reference to a record by its public id. The engine resolves the id
/// inside the transaction, so records staged earlier in the same transaction
/// can be referenced too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecordReference {
    /// The record id (dashed UUID or compact hex).
    pub id: String,
}

/// Definition of a declared (non-vector) index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexDefinition {
    /// Tenant-unique index name.
    pub name: String,
    /// Label whose records are indexed.
    pub label: String,
    /// Indexed property.
    pub property: String,
    /// Index kind: `ordered` (equality, `$in`, ranges, `startsWith`),
    /// `text` (BM25 full-text; `property` may be `*` for every string
    /// property of the record) or `structural` (one vector per record from
    /// the graph around it, searched with `query.vector`; `property` is `*`).
    #[serde(default = "IndexDefinition::default_kind")]
    pub kind: String,
    /// Text indexes only: `eventual` (default; searches see what the applier
    /// has indexed, typically within 250 ms) or `immediate` (read-your-writes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<String>,
    /// Structural indexes only: vector dimensions (8..=4096).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<u32>,
}

impl IndexDefinition {
    fn default_kind() -> String {
        "ordered".into()
    }
}

/// Definition of a vector index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexDefinition {
    /// Label whose records are indexed.
    pub label: String,
    /// Property holding the vector.
    pub property: String,
    /// Vector dimensionality.
    pub dimensions: u32,
    /// Similarity metric name.
    #[serde(default = "default_similarity")]
    pub similarity: String,
    /// Stable opaque index name (the source of truth is the metadata record:
    /// label/property/similarity/dimensions). When absent the engine derives a
    /// deterministic name from the definition for legacy compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Optional caller-declared embedding provenance (model id/revision). The
    /// engine does not interpret or verify it, but rejects an accidental
    /// change under an existing name so incomparable vectors cannot be mixed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Owner kind the index covers: `"record"` (default) or `"relationship"`.

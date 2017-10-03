//! Embedded database node shared by protocol executors and transports.
//! All engines use one store and every write flows through deterministic
//! commit batches.

mod adapters;
mod backup;
mod catalog;
mod facets;
mod fusion;
mod hybrid;
mod indexes;
mod ingest;
mod metrics;
mod observability;
mod queries;
mod records;
mod relationship_reads;
mod writer_holds;
pub(crate) use records::UnlabeledUpsert;
mod relationships;
mod residency;
mod retention;
pub(crate) mod scope;
mod suggestions;
mod transactions;
mod usage;
pub use usage::UsageError;
mod vector_index_meta;
mod vector_mode;
mod vectors;
pub(crate) mod working_set;

use scope::RequestScope;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rushdb_lmpg::import;
use rushdb_lmpg::reader::try_access as access;
use rushdb_query::SearchRequest;
use rushdb_types::ids::{OwnerKind, RecordId, RelationshipId, TenantId};
use rushdb_types::keys::{key, P_FAMILY_SIG};

type PropertyMetadata =
    BTreeMap<String, Vec<(u64, String, String, rushdb_types::model::LogicalType)>>;
type IndexedRecord = (RecordId, u64, serde_json::Map<String, serde_json::Value>);

type PropertyAggregate = (String, u64);
type RelationshipAggregate = (u64, BTreeMap<String, PropertyAggregate>);

const MAX_CURSOR_PAGE_SIZE: u64 = 10_000;
/// Node-wide cap for serialized cursor request state retained alongside pinned
/// read views. The view itself is owned by storage and is bounded separately.
const MAX_RESIDENT_CURSOR_BYTES: usize = 64 * 1024 * 1024;
/// A Protocol v1 cursor holds a storage read view.  It is deliberately kept
/// server-local: its opaque id has no meaning after a process restart.
struct ResultCursor {
    tenant: TenantId,
    /// Policy generation captured when the cursor was created. A context-bound
    /// continuation must use the same generation or fail closed.
    policy_generation: u64,
    /// Ephemeral retention-consumer identity. Cursors die with the process,
    /// so this registration is intentionally not persisted with metadata.
    retention_consumer: String,
    view: Arc<rushdb_storage::ReadView>,
    search: SearchRequest,
    next_offset: u64,
    /// Total matches at cursor creation. The view is pinned, so the total
    /// is invariant across pages: continuations reuse it instead of
    /// re-scanning the match set for a count on every page.
    total: u64,
    expires_at: Instant,
    /// Shared logical-cache reservation for the serialized request state.
    /// Dropping the cursor releases this charge automatically.
    _reservation: Option<rushdb_storage::resources::ByteReservation>,
}

fn cursor_resident_bytes(cursor: &ResultCursor) -> usize {
    // SearchRequest is the variable-sized part of a cursor. Include a fixed
    // allowance for the cursor metadata and map/allocator overhead; the pinned
    // read view is accounted by storage's snapshot budget.
    serde_json::to_vec(&cursor.search).map_or(256, |bytes| bytes.len().saturating_add(256))
}

fn append_bounded_json_response(
    values: &mut Vec<serde_json::Value>,
    encoded_bytes: &mut usize,
    value: serde_json::Value,
    limit: usize,
) -> Result<(), String> {
    let bytes =
        serde_json::to_vec(&value).map_err(|error| format!("encode response row: {error}"))?;
    let next = encoded_bytes.saturating_add(bytes.len());
    if next > limit {
        return Err(format!(
            "response exceeds configured byte limit ({next} > {limit})"
        ));
    }
    *encoded_bytes = next;
    values.push(value);
    Ok(())
}

pub fn hex_simple(id: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    id.iter().fold(String::with_capacity(32), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// Map a similarity metric string to the engine enum (cosine/dot/euclidean).
pub fn similarity_from_str(s: &str) -> Result<rushdb_types::occurrence::Similarity, String> {
    match s.to_ascii_lowercase().as_str() {
        "cosine" => Ok(rushdb_types::occurrence::Similarity::Cosine),
        "dot" => Ok(rushdb_types::occurrence::Similarity::Dot),
        "euclidean" | "l2" => Ok(rushdb_types::occurrence::Similarity::Euclidean),
        other => Err(format!(
            "unknown similarity metric `{other}` (cosine|dot|euclidean)"
        )),
    }
}

/// Validate a caller-supplied vector index name. The alias is an opaque
/// compatibility key and is never parsed, but a bounded, unparsed-safe charset
/// keeps it addressable and stops custom names from colliding with the engine's
/// derived `emb__...` form. `derived` is the alias this definition would get
/// without a name, which is allowed to use the reserved prefix.
fn validate_index_alias(name: &str, derived: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err("vector index name must be 1..=64 bytes".into());
    }
    if name.starts_with("emb__") && name != derived {
        return Err("vector index name must not use the reserved `emb__` prefix".into());
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
    {
        return Err("vector index name may contain only [A-Za-z0-9._:-]".into());
    }
    Ok(())
}

/// Normalize a vector index owner scope to `"record"` or `"relationship"`.
fn normalize_vector_owner(owner: Option<&str>) -> Result<&'static str, String> {
    match owner.map(str::to_ascii_lowercase).as_deref() {
        None | Some("record" | "node") => Ok("record"),
        Some("relationship" | "edge") => Ok("relationship"),
        Some(other) => Err(format!(
            "unknown vector index owner `{other}` (record|relationship)"
        )),
    }
}

/// Cached `(labels, per-label counts)` for one tenant at one commit index.
type CachedMetadata = std::sync::Arc<(Vec<String>, Vec<(String, u64)>)>;

/// Normalized property name -> declared type name (the property dictionary).
type PropertyTypeMap = std::collections::BTreeMap<String, String>;

/// A byte-accounted logical cache charged to the shared budget tree.
type LogicalCache<K, V> = rushdb_storage::cache::LogicalCache<K, V>;

/// Commit/policy-generation-keyed `(labels, counts)` logical cache.
type MetadataCache = std::sync::Mutex<Option<LogicalCache<(TenantId, u64), (u64, CachedMetadata)>>>;

/// Commit/policy-generation-keyed property-dictionary logical cache.
type PropertyCache =
    std::sync::Mutex<Option<LogicalCache<(TenantId, u64, u64), std::sync::Arc<PropertyTypeMap>>>>;

/// Embedded database node: one store plus the per-tenant engine instances,
/// metrics, request-scope state, and maintenance caches shared by every
/// transport adapter.
pub struct Node {
    store: Arc<rushdb_storage::Store>,
    base_tenant: TenantId,
    /// When true, a request scope (`X-RushDB-Tenant` / pgwire `database`) is
    /// required and selects the tenant keyspace via the full SHA-256 of the
    /// scope value — collision-free, so no discriminator property is ever
    /// stored on records. `false` is single-tenant mode: no scope, one
    /// fixed keyspace.
    multitenant: bool,
    maintenance: bool,
    transactions: crate::resource_config::TransactionConfig,
    languages: rushdb_protocol::LanguageSet,
    /// Per-tenant in-memory handles, admitted on demand and evicted when
    /// idle.
    residency: residency::TenantResidency,
    /// Emit per-tenant metric series (bounded by `metrics_tenant_limit`).
    metrics_per_tenant: Option<usize>,
    /// WAL archive directory: base backups register there so archive
    /// retention keeps what they need.
    wal_archive_dir: Option<std::path::PathBuf>,
    /// Declared text indexes of every tenant.
    text: std::sync::Arc<rushdb_textsearch::TextIndexes>,
    metrics: Metrics,
    bindings: Vec<(String, u16, bool)>,
    max_request_bytes: usize,
    cursors: std::sync::Mutex<HashMap<String, ResultCursor>>,
    cursor_bytes: std::sync::Mutex<usize>,
    /// Byte-accounted logical cache for `(labels, per-label counts)` keyed by
    /// tenant and policy generation. Pinned views make these results pure
    /// over the commit index, so entries are exact until the next commit or
    /// policy rotation. `None` when `resources.logical-cache-bytes` is 0.
    metadata_cache: MetadataCache,
    /// Byte-accounted logical cache for the property dictionary (normalized
    /// name -> type), keyed by `(tenant, commit index, policy generation)`.
    property_cache: PropertyCache,
    /// Last computed [`rushdb_storage::StorageStatistics`] and when. Refreshed
    /// at most once per scrape interval so `/metrics` never scans per scrape.
    storage_stats:
        std::sync::Mutex<Option<(std::time::Instant, rushdb_storage::StorageStatistics)>>,
    /// Cached per-tenant record-header counts and when. Refreshed off the
    /// request path (the count scans the whole keyspace).
    records_by_tenant_cache:
        std::sync::Mutex<Option<(std::time::Instant, std::collections::BTreeMap<u64, u64>)>>,
    /// Slow-call log threshold in milliseconds; 0 disables the log.
    slow_call_threshold_ms: u64,
    /// Per-tenant share of concurrent bulk requests.
    bulk_share: crate::bulk_share::BulkShare,
    /// Prefix applied to exported Prometheus metric names.
    metrics_prefix: Option<String>,
    /// Only export metric families whose name contains this substring.
    metrics_filter: Option<String>,
    /// Readiness fails below this free-disk percentage; 0 disables the guard.
    disk_space_threshold_percent: u8,
    /// Server-side request deadline in milliseconds (0 = none), applied to
    /// every transport by the protocol executor.
    request_timeout_ms: u64,
    /// Data directory the disk guard inspects (absent for memory stores).
    data_dir: Option<std::path::PathBuf>,
    /// Address advertised in build info; `None` echoes the bind address.
    advertised_address: Option<String>,
    /// Require an explicit index name on vector writes and searches (no
    /// derived `emb__...` alias fallback).
    require_vector_index_name: bool,
    /// See `resources.vector-exact-max-pool`.
    vector_exact_max_pool: u64,
    /// See `resources.vector-default-ef`.
    vector_default_ef: usize,
    /// See `resources.introspection-value-max-chars`.
    introspection_value_max_chars: usize,
    /// Acknowledged floors for active transaction/replica/backup/index
    /// consumers. Slow consumers protect history from retention pruning.
    retention_consumers: std::sync::Mutex<crate::retention::RetentionConsumers>,
    /// Owns the optional Raft runtime so it shuts down with the node instead
    /// of being leaked after bootstrap.
    raft_runtime: Option<tokio::runtime::Runtime>,
    /// Bounded blocking executor shared by network transports; embedded
    /// callers use the scheduler owned by `rushdb_engine::Engine` instead.
    scheduler: std::sync::Arc<rushdb_engine::BoundedScheduler>,
    /// `true` while a `--replica-of` task is polling/applying (streaming
    /// replication); writes are rejected meanwhile. Only the task clears it (never
    /// `request_promotion` directly), so a stale batch can never still be
    /// mid-apply once writes are admitted.
    following: std::sync::atomic::AtomicBool,
    /// Operator request (`promote`) the task observes at its next poll.
    follower_stop_requested: std::sync::atomic::AtomicBool,
    /// Maintenance closures on the blocking pool; shutdown drains them before
    /// the final checkpoint.
    maintenance_work: crate::lifecycle::MaintenanceWork,
    /// Wakes the index builder when an index is declared or dropped.
    index_build_wake: std::sync::Arc<tokio::sync::Notify>,
}

impl Drop for Node {
    fn drop(&mut self) {
        // The replicator owns the OpenRaft cluster, whose state-machine list
        // includes this store. Clear that back-edge before stopping the
        // runtime or the store remains in a strong cycle and redb cannot be
        // reopened in the same process.
        self.store.set_replicator(None);
        if let Some(runtime) = self.raft_runtime.take() {
            // OpenRaft tasks retain the store/backend while they wind down.
            // Waiting here is required before a same-process reopen; the
            // background-only shutdown path can return while redb still holds
            // its process lock.
            runtime.shutdown_timeout(std::time::Duration::from_secs(5));
        }
    }
}

use metrics::{disk_free_ok, Metrics};

/// Per-property statistics collected during the describe scan. `min`/`max`
/// use `serde_json`'s scalar ordering (numeric for numbers, lexical for
/// strings/ISO datetimes); `samples` is a bounded set of distinct scalars.
#[derive(Default)]
struct PropertyScanStats {
    records_count: usize,
    is_array: bool,
    samples: Vec<serde_json::Value>,
    min: Option<serde_json::Value>,
    max: Option<serde_json::Value>,
}

/// Bounded number of distinct sample values surfaced by `describe_properties`.
const SCHEMA_SAMPLE_CAP: usize = 10;

/// Index-derived value domain for a property (see
/// [`Node::value_domain_from_index`]).
#[derive(Debug, Clone)]
pub(crate) struct ValueDomainIndex {
    /// Exact distinct scalar values from the equality postings.
    pub distinct: u64,
    /// Whether an explicit-null occurrence exists (null is one distinct value).
    pub null_present: bool,
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
    /// True when every known definition is numeric, so `min`/`max` are valid.
    pub numeric_only: bool,
}

impl ValueDomainIndex {
    pub(crate) fn distinct_total(&self) -> u64 {
        self.distinct + u64::from(self.null_present)

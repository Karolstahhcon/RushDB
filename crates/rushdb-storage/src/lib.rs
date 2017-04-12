//! Ordered KV substrate shared by every node: buffered transactions over a
//! committed B-tree view, atomic redb publication of expanded op sets, a
//! serialized command log, multi-version snapshot reads, and
//! serializable-optimistic dependency validation.

// The storage boundary deliberately exposes byte-oriented iterator and callback
// signatures. Naming each nested trait-object form would obscure that API.
#![allow(clippy::type_complexity)]

mod adapter_budgets;
pub mod backend;
pub mod cache;
mod health;
pub mod read_accounting;
mod transaction_view;
pub use transaction_view::Txn;
use transaction_view::TxnBase;
mod commit;
mod copy_on_write;
pub use copy_on_write::CopyOnWrite;
mod conflict_window;
mod derive;
mod fair_gate;
pub mod history_v2;
mod layout;
mod log_export;
pub mod log_store;
mod maintenance;
mod memory_backend;
mod observability;
mod overlay_view;
mod persist;
pub mod planner_stats;
mod publish;
mod read_view;
mod receipt;
mod redb_backend;
pub mod resources;
mod temp_path;
pub mod tenant_dir;
mod usage;
pub use temp_path::TempPath;
pub use usage::is_usage_bytes_key;
pub mod wal;
mod wal_replay;
use backend::StorageBackend;
pub use derive::{DeriveRead, Deriver};
pub use maintenance::CompactOutcome;
pub use observability::{
    BackendLockTimes, CommitObserver, CommitPhase, WriterHolder, WriterHolds, WRITER_HOLD_BOUNDS,
};
pub use overlay_view::OverlayView;
pub use planner_stats::PlannerStats;
pub use read_view::{
    ReadCancellation, ReadLimits, ReadPin, ReadView, ScanBatch, ScanBounds, ScanCursor,
    ScanProjection, ScanRow,
};

#[cfg(test)]
mod derive_tests;
#[cfg(test)]
mod read_view_tests;
#[cfg(test)]
mod receipt_tests;
#[cfg(test)]
mod regression_tests;
#[cfg(test)]
mod sequencer_tests;
#[cfg(test)]
mod writer_tests;

/// Remove the materialization file at `path`. The WAL directory and layout
/// manifest are left alone.
///
/// # Errors
/// Returns the I/O error of the removal.
pub fn remove_store(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::remove_file(path)
}

use rushdb_types::command::CommitBatch;
use rushdb_types::ids::TenantId;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Included, Unbounded};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use thiserror::Error;
use uuid::Uuid;

/// Per-tenant outer admission shards, retained for replication call ordering.
/// Local expansion additionally takes the single logical writer mutex.
const N_SHARDS: usize = 64;

/// A fully-admitted op-set awaiting batched durable publication, keyed by
/// its reserved commit index. The publisher drains the contiguous prefix from
/// the published watermark in one redb write transaction.
struct PendingCommit {
    ci: u64,
    /// Physical shard this commit belongs to (tenant routing). The publisher
    /// fans out durability across shard backends so different tenants persist
    /// in parallel once `shard_count > 1`.
    shard: usize,
    ops: Vec<Op>,
    prepared: PreparedCommand,
    /// The originating `CommitBatch`, pre-serialized: paired with
    /// `ops` in the WAL record so the command journal is derivable from the
    /// WAL alone, without a separate `__log__` redb row.
    command_payload: Vec<u8>,
    ack: mpsc::Sender<Result<(), StorageError>>,
}

/// Pending-overlay state for the commit sequencer.
///
/// A committed `base` plus the net key effect of every assigned-but-unpublished
/// commit, so an expansion can read through `Txn`'s overlay instead of waiting
/// for the predecessor to be published. It is guarded by the writer
/// `admission_lock`, so every writer that can assign a commit index observes
/// one consistent overlay.
///
/// `buffer` is this state's *own* copy (never the publisher's `pending` map), so
/// it cannot race the publisher removing a drained entry. Rebase only needs the
/// generation (commit index) at which each key was last written, so the buffer
/// stays O(distinct in-flight keys), not O(in-flight ops).
#[derive(Default)]
struct SequencerState {
    /// `key -> (commit index that last wrote it, value or tombstone)` for
    /// assigned-but-unpublished commits. The committed base is opened fresh
    /// (and dropped) per critical section, so this state holds no read pin
    /// between commits and cannot stall GC.
    buffer: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
}

impl std::fmt::Debug for SequencerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SequencerState")
            .field("in_flight_keys", &self.buffer.len())
            .finish()
    }
}

impl SequencerState {
    /// Fold an assigned commit's expanded ops into the pending buffer.
    fn record(&mut self, ci: u64, ops: &[Op]) {
        for op in ops {
            match op {
                Op::Put(key, value) | Op::Set(key, value) => {
                    self.buffer.insert(key.clone(), (ci, Some(value.clone())));
                }
                Op::Del(key) | Op::Purge(key) => {
                    self.buffer.insert(key.clone(), (ci, None));
                }
            }
        }
    }
}

pub(crate) const LOG_PREFIX: &[u8] = b"__log__/";
const LAST_APPLIED_KEY: &[u8] = b"meta/last-applied";
/// Durable watermark: the highest LSN retention has retired.
/// Bounds `Store::truncate_log`, so the retention floor (hard caps and
/// consumer protection) decides which WAL segments may go.
pub(crate) const RETENTION_CURSOR_KEY: &[u8] = b"meta/retention-cursor";

/// Durable byte accounting by key family, for growth diagnosis and operator
/// metrics. All values are exact sums of stored key+value lengths for the
/// family, taken from one read view; they exclude redb's copy-on-write slack
/// (compare against [`Store::storage_file_bytes`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageStatistics {
    /// Canonical data rows (records, occurrences, postings, catalogs, …).
    pub data_keys: u64,
    /// Total key+value bytes of canonical data rows.
    pub data_bytes: u64,
    /// Retained MVCC history rows.
    pub history_keys: u64,
    /// Total key+encoded-history bytes.
    pub history_bytes: u64,
    /// Retained command-log rows (`__log__/<lsn>`).
    pub log_keys: u64,
    /// Total command-log bytes.
    pub log_bytes: u64,
    /// Retry-receipt rows (receipts plus their expiry index).
    pub receipt_keys: u64,
    /// Total retry-receipt bytes.
    pub receipt_bytes: u64,
    /// Number of bounded admission-lock waits (including timed-out waits).
    pub admission_waits: u64,
    /// Milliseconds spent waiting for admission-lock ownership.
    pub admission_wait_ms: u64,
    /// Number of explicit durability checkpoints.
    pub checkpoints: u64,
    /// Milliseconds spent in explicit durability checkpoints.
    pub checkpoint_wait_ms: u64,
}

/// Errors returned by the storage layer: failed preconditions, corrupt or
/// unsupported durable state, resource exhaustion, and invalid read requests.
#[derive(Debug, Clone, Error)]
pub enum StorageError {
    /// A poisoned storage lock fenced the store; reopen is required.
    #[error("storage unhealthy after lock poisoning; reopen required")]
    Unhealthy,
    /// A transaction precondition was violated; `path` is the offending key.
    #[error("precondition failed on {path}")]
    Precondition {
        /// Key whose precondition failed.
        path: String,
    },
    /// The deterministic expander failed while building a commit.
    #[error("builder error: {0}")]
    Builder(String),
    /// A durable record could not be decoded or verified.
    #[error("corrupt log entry")]
    CorruptLog,
    /// A log record carries a command version this build does not support.
    #[error("unsupported command version {0}")]
    UnsupportedCommandVersion(u32),
    /// The stored outcome payload is missing or cannot be deserialized.
    #[error("committed outcome payload is unavailable or incompatible")]
    OutcomePayload,
    /// The physical backend failed while applying or reading durable state.
    #[error("durable storage failure: {0}")]
    Durability(String),
    /// The requested snapshot predates the oldest retained commit.
    #[error("snapshot {requested} expired; oldest supported commit is {oldest}")]
    SnapshotExpired {
        /// Snapshot commit index that was requested.
        requested: u64,
        /// Oldest commit index still readable.
        oldest: u64,
    },
    /// The requested snapshot is newer than the visible commit index.
    #[error("snapshot {requested} is newer than visible commit {visible}")]
    FutureSnapshot {
        /// Snapshot commit index that was requested.
        requested: u64,
        /// Commit index currently visible.
        visible: u64,
    },
    /// A managed byte budget refused the request.
    #[error("storage resource exhausted: {0}")]
    ResourceExhausted(String),
    /// The supplied storage configuration is not valid.
    #[error("invalid storage configuration: {0}")]
    InvalidConfiguration(String),
    /// A pinned read was cancelled by its caller.
    #[error("storage read cancelled")]
    Cancelled,
    /// Scan bounds or batch limits are invalid.
    #[error("invalid scan bounds or limits: {0}")]
    InvalidRead(String),
    /// Physical compaction was requested while protected read views are open.
    #[error("physical compaction requires all protected read views to close")]
    ReadersActive,
    /// No conflict summary is retained for a commit that must be probed.
    #[error(
        "actual write summary unavailable for commit {commit_index}; retry with a fresh snapshot"
    )]
    ConflictHistoryUnavailable {
        /// Commit index whose conflict summary is missing.
        commit_index: u64,
    },
}

/// Operation emitted into the atomic publish set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Insert or overwrite a key, retaining the superseded value as history.
    Put(Vec<u8>, Vec<u8>),
    /// Remove a key, retaining the removed value as history.
    Del(Vec<u8>),
    /// Overwrite a key **without** retaining a superseded version (and drop
    /// any history already accumulated for it). Used for internal bookkeeping
    /// keys whose values are not part of user-visible time travel: counters,
    /// allocation watermarks, degree counters, the label catalog and the
    /// commit watermark. Without this, every overwrite of a hot counter
    /// rewrites its whole history vector, which is quadratic in the number of
    /// writes.
    Set(Vec<u8>, Vec<u8>),
    /// Erase a key together with its current-value MVCC metadata and retained
    /// history, without appending a new version. Destructive by design: used
    /// by tenant purge, where the values are intentionally destroyed and
    /// keeping versions for them would bloat storage without bound.
    Purge(Vec<u8>),
}

impl Op {
    /// Approximate encoded size (key + value bytes) for group-commit byte
    /// accounting (P04 bounded writer).
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        match self {
            Op::Put(k, v) | Op::Set(k, v) => k.len() + v.len(),
            Op::Del(k) | Op::Purge(k) => k.len(),
        }
    }
}

/// Reason prefix of a [`CommitOutcome::Conflicted`] caused by a read
/// dependency that a later commit wrote (first-committer-wins).
pub const SERIALIZABLE_CONFLICT: &str = "serializable_conflict";

/// Serializable-mode read dependencies recorded against a transaction.
/// Zero-width entries are point reads; otherwise half-open [start,end).
#[derive(Default, Clone, Debug)]
pub struct DependencySet {
    /// Recorded read ranges as `(start, end)` pairs; a zero-width pair is a
    /// point dependency.
    pub ranges: Vec<(Vec<u8>, Vec<u8>)>,
    whole_keyspace: bool,
}

impl DependencySet {
    /// Whether no point, range, or whole-keyspace dependency was recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty() && !self.whole_keyspace
    }

    /// Record a half-open `[start, end)` range dependency.
    pub fn record(&mut self, start: Vec<u8>, end: Vec<u8>) {
        self.ranges.push((start, end));
    }

    /// Prefix tracking is explicit so an empty prefix (all keys) is distinct
    /// from a point dependency on the empty key.
    pub fn record_prefix(&mut self, prefix: &[u8]) {
        if prefix.is_empty() {
            self.whole_keyspace = true;
        } else {
            let (start, end) = prefix_range(prefix);
            self.record(start, end);
        }
    }

    /// Merge dependencies captured by another preparation phase.
    pub fn extend(&mut self, other: Self) {
        self.ranges.extend(other.ranges);
        self.whole_keyspace |= other.whole_keyspace;
    }

    fn intersects(&self, key: &[u8]) -> bool {
        self.whole_keyspace
            || self.ranges.iter().any(|(s, e)| {
                if s == e {
                    key == s.as_slice()
                } else {
                    key >= s.as_slice() && (e.is_empty() || key < e.as_slice())
                }
            })
    }

    /// Whether `key` falls in any recorded read range (serializable conflict
    /// probe for transaction commit).
    #[must_use]
    pub fn intersects_key(&self, key: &[u8]) -> bool {
        self.intersects(key)
    }
}

/// Prepared metadata for one admitted command. The publisher keeps this
/// alongside expanded operations so an admission overlay can validate
/// read/write conflicts without re-expanding the command.
#[derive(Clone, Debug, Default)]
pub struct PreparedCommand {
    /// Reserved commit index, preserving command-log order.
    pub commit_index: u64,
    /// Canonical keys written by the expanded operation set.
    pub writes: BTreeSet<Vec<u8>>,
    /// Keys/ranges read while preparing the command.
    pub reads: DependencySet,
    /// Expanded under writer admission against the sequencer overlay, so its
    /// reads already observed every lower-index commit (published or
    /// pending). Such a command can share a publish group with the commits
    /// it read from: the group applies in commit-index order.
    pub sequenced: bool,
}

impl PreparedCommand {
    /// Build prepared metadata from an expanded operation set.
    #[must_use]
    pub fn from_ops(commit_index: u64, ops: &[Op]) -> Self {
        let writes = ops
            .iter()
            .map(|op| match op {
                Op::Put(key, _) | Op::Set(key, _) | Op::Del(key) | Op::Purge(key) => key,
            })
            .cloned()
            .collect();
        Self {
            commit_index,
            writes,
            reads: DependencySet::default(),
            sequenced: false,
        }
    }

    /// Whether this command's reads overlap writes from `other`, or vice
    /// versa. The check is symmetric and suitable for optimistic admission.
    ///
    /// The write-write check (large-transaction support, Part F) uses
    /// `writes` being a `BTreeSet` directly — `other.writes.contains(key)`
    /// is O(log n), not the nested linear scan this used to be. Filtering
    /// only `self`'s side is still exactly equivalent to filtering both:
    /// `is_coordination_write` is a pure function of the key's own bytes, so
    /// if `key` already passed the filter, an identical byte sequence in
    /// `other.writes` has the same classification — there is no case where
    /// filtering the other side too would change the answer.
    ///
    /// Read sets are probed **against** the ordered write set — one range
    /// query per recorded read — rather than testing every written key
    /// against a linear list of read ranges. The old form was
    /// O(writes × reads) per pair and, applied pairwise inside every publish
    /// group, starved bulk ingest for minutes (cloud-capacity F4 follow-up).
    #[must_use]
    pub fn conflicts_with(&self, other: &Self) -> bool {
        reads_hit_writes(&other.reads, &self.writes)
            || reads_hit_writes(&self.reads, &other.writes)
            || {
                let (small, large) = if self.writes.len() <= other.writes.len() {
                    (&self.writes, &other.writes)
                } else {
                    (&other.writes, &self.writes)
                };
                small
                    .iter()
                    .filter(|key| !is_coordination_write(key))
                    .any(|key| large.contains(key))
            }
    }
}

/// Whether any recorded read (point, range, or whole keyspace) covers a
/// non-coordination key of `writes`. O(reads × log writes).
fn reads_hit_writes(reads: &DependencySet, writes: &BTreeSet<Vec<u8>>) -> bool {
    use std::ops::Bound::Excluded;
    let application = |key: &&Vec<u8>| !is_coordination_write(key);
    if reads.whole_keyspace {
        return writes.iter().any(|key| application(&key));
    }
    reads.ranges.iter().any(|(start, end)| {
        if start == end {
            writes.contains(start) && !is_coordination_write(start)
        } else if end.is_empty() {
            writes
                .range::<[u8], _>((Included(start.as_slice()), Unbounded))
                .any(|key| application(&key))
        } else if start < end {
            writes
                .range::<[u8], _>((Included(start.as_slice()), Excluded(end.as_slice())))
                .any(|key| application(&key))
        } else {
            false
        }
    })
}

/// Ordered publication owns these metadata rows; they are rewritten by every
/// commit as part of the command log and do not represent application-level
/// read/write overlap for group admission.
fn is_coordination_write(key: &[u8]) -> bool {
    key == LAST_APPLIED_KEY
        || key.starts_with(LOG_PREFIX)
        || key.starts_with(receipt::RECEIPT_PREFIX)
        || key.starts_with(receipt::RECEIPT_EXP_PREFIX)
}

/// Inclusive start / exclusive end range covering all keys under a prefix.
/// An empty end bound means unbounded above.
#[must_use]
pub fn prefix_range(prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut end = prefix.to_vec();
    loop {
        match end.last_mut() {
            Some(last) if *last != 0xFF => {
                *last += 1;
                return (prefix.to_vec(), end);
            }
            Some(_) => {
                end.pop();
            }
            None => return (prefix.to_vec(), Vec::new()),
        }
    }
}

fn log_key(seq: u64) -> Vec<u8> {
    let mut k = LOG_PREFIX.to_vec();
    k.extend_from_slice(format!("{seq:020}").as_bytes());
    k
}

/// Terminal status of a commit key, as stored durably.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeKind {
    /// The commit succeeded at the given commit index.
    Committed(u64),
    /// The commit was terminally aborted and must not be retried under the

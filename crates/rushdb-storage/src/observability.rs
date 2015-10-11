//! Live store observability hooks: per-commit latency, backend lock
//! contention, and the conflict window's footprint (C0, cloud-capacity F12).

use std::sync::Arc;

use super::Store;

/// One timed step of the commit pipeline. Committer-side phases are
/// observed once per commit; publisher-side phases once per publish group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommitPhase {
    /// Waiting for the admission (sequencer) lock.
    AdmissionWait,
    /// Running the caller's planner/build closure.
    Build,
    /// Deterministic expansion into ops, including slot-reservation retries.
    Expand,
    /// Enqueue until the durable acknowledgement (the end-to-end latency the
    /// client waits for after expansion).
    Durable,
    /// Publisher: encoding a group's WAL records.
    WalEncode,
    /// Publisher: appending and fsyncing a group's WAL records.
    WalAppendSync,
    /// Publisher: applying a group's ops to the backend materialization.
    BackendApply,
    /// Publisher: advancing derived state for a group.
    Derive,
    /// Publisher: conflict-window recording, watermark advance and acks.
    Publish,
}

impl CommitPhase {
    /// Every phase, in pipeline order.
    pub const ALL: [Self; 9] = [
        Self::AdmissionWait,
        Self::Build,
        Self::Expand,
        Self::Durable,
        Self::WalEncode,
        Self::WalAppendSync,
        Self::BackendApply,
        Self::Derive,
        Self::Publish,
    ];

    /// Stable metric label for this phase.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::AdmissionWait => "admission_wait",
            Self::Build => "build",
            Self::Expand => "expand",
            Self::Durable => "durable",
            Self::WalEncode => "wal_encode",
            Self::WalAppendSync => "wal_append_sync",
            Self::BackendApply => "backend_apply",
            Self::Derive => "derive",
            Self::Publish => "publish",
        }
    }
}

/// Callback receiving each timed commit phase, in seconds.
pub type CommitObserver = dyn Fn(CommitPhase, f64) + Send + Sync;

/// Cumulative backend database-lock timings, summed over every shard.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendLockTimes {
    /// Nanoseconds spent waiting to acquire the lock.
    pub wait_ns: u64,
    /// Nanoseconds the lock was held.
    pub hold_ns: u64,
    /// Longest single hold, in nanoseconds.
    pub hold_max_ns: u64,
}

/// What holds a backend's single writer: one redb write transaction, from
/// `begin_write` to its commit. Every other writer waits meanwhile, so the
/// longest holds are what stall other tenants' commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterHolder {
    /// Publishing commit groups into the materialization.
    Publish,
    /// One batch of history garbage collection.
    HistoryGc,
    /// A durable checkpoint (fsync).
    Checkpoint,
    /// Writes paused for a consistent copy (backup).
    Pause,
}

impl WriterHolder {
    /// Every holder.
    pub const ALL: [Self; 4] = [
        Self::Publish,
        Self::HistoryGc,
        Self::Checkpoint,
        Self::Pause,
    ];

    /// Stable metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::HistoryGc => "history_gc",

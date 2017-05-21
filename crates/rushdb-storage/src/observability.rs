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

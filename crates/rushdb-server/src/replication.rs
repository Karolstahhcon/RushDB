//! Streaming replication: a follower node tails a leader's
//! WAL and replays each committed batch locally through
//! [`rushdb_storage::Store::apply_replicated_batch_with_payload`]. Derived
//! state is never shipped — only recomputed, deterministically, from the
//! same mutations the leader applied.
//!
//! A follower starts from an empty store (tailing from LSN 1) or from a
//! base backup of the leader (mid-stream attach, tailing from the backup's
//! commit index). A follower's local commit index is assigned sequentially
//! by `apply_replicated_batch`, so it stays aligned with the leader's LSN
//! numbering only when every batch after its starting point is replayed in
//! order, with no gaps (see [`rushdb_storage::Store::try_log_commands_from`]'s
//! doc comment for why normal operation never produces a gap).
//!
//! Tailing from LSN 1 needs the leader to still hold that history. The
//! default retention keeps only what a registered consumer pins (DC4,
//! 2026-10-10), and a follower registers on its first poll, so a follower
//! that starts empty after the leader has pruned gets `resync_required` and
//! must attach from a base backup instead. Leaders that serve
//! bootstrap-from-empty followers set `tx-log-keep-commits`.
//!
//! Internal, HTTP-only mechanism — not part of the client-facing protocol
//! surface, the same way `Node::export_snapshot_at`/`restore_pitr_to` are
//! internal.

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;

use crate::node::Node;
use crate::retention::ReplicaConfig;

/// Bound on records served per poll, keeping one response (and the backing
/// WAL read) a reasonable size even when a follower is far behind.
const WAL_TAIL_PAGE_LIMIT: usize = 2_000;

/// How long a caught-up (or errored) follower waits before polling again.
const FOLLOWER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// One WAL record ready to replay: an LSN and its base64-encoded,
/// JSON-serialized `CommitBatch`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalRecord {
    /// The commit's log sequence number on the leader.
    pub lsn: u64,
    /// Base64-encoded JSON `CommitBatch`.
    pub command_b64: String,
}

/// Leader response to a follower's WAL-tail poll.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalTailResponse {
    /// Records with LSN `>= since`, in order, bounded to
    /// [`WAL_TAIL_PAGE_LIMIT`].
    pub records: Vec<WalRecord>,
    /// `true` when `records` is everything the leader currently has for
    /// this follower (no page limit was hit).
    pub caught_up: bool,
}

/// Why a WAL-tail poll could not be served.
#[derive(Debug)]
pub(crate) enum WalTailError {
    /// The follower asked for history retention already reclaimed:
    /// it must be rebuilt from a fresh base backup.
    Resync(crate::retention::ConsumerResyncRequired),
    /// The WAL read failed.
    Read(String),
}

/// Build the leader-side response to a follower's poll: registers the
/// follower's retention floor at `since` *before* reading (so a concurrent
/// GC tick cannot truncate a segment this poll is about to serve), then
/// reads and bounds the result to [`WAL_TAIL_PAGE_LIMIT`].
///
/// # Errors
/// [`WalTailError::Resync`] when `since` is below the retention floor;

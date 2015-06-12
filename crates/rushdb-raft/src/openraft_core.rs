//! OpenRaft-backed consensus core for canonical `RushDB` command batches.
//!
//! Networking is an in-process adapter used by deterministic engine tests.
//! TCP/HTTP transports only forward `OpenRaft` RPC types; they do not implement
//! election, term, membership, or replication logic themselves.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Adaptor;
use openraft::{
    Entry, EntryPayload, LogId, LogState, OptionalSend, Raft, RaftLogReader, RaftStorage, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use openraft_memstore::{ClientRequest, ClientResponse, TypeConfig};
use tokio::sync::RwLock;

/// Raft node identifier; `RushDB` uses dense `u64` indices as voter IDs.
pub type NodeId = u64;
/// The concrete `OpenRaft` handle parameterized by the in-memory type config.
pub type OpenRaft = Raft<TypeConfig>;
type Registry = Arc<RwLock<HashMap<NodeId, OpenRaft>>>;

/// A commit batch plus the leader's serialized response payload, carried
/// together through the raft log so every node stores the same outcome.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplicatedCommit {
    /// Canonical command batch produced by `RushDB`'s local admission path.
    pub batch: rushdb_types::command::CommitBatch,
    /// Leader's serialized response payload, replayed verbatim on followers.
    pub payload: Vec<u8>,
}

/// OpenRaft-backed `rushdb_storage::Replicator` seam: local admission builds a
/// batch, then `replicate` sends it to the leader and returns once the quorum
/// has durably applied it (the caller's own store applies via the raft state
/// machine, so the outcome + payload become visible before returning).
#[derive(Clone)]
pub struct ClusterReplicator {
    cluster: Arc<OpenRaftCluster>,
    runtime: tokio::runtime::Handle,
    acknowledged: Arc<std::sync::atomic::AtomicU64>,
}

impl ClusterReplicator {
    /// Wrap a cluster handle and the runtime that drives it so local commits
    /// can be routed through the current leader.
    #[must_use]
    pub fn new(cluster: Arc<OpenRaftCluster>, runtime: tokio::runtime::Handle) -> Self {
        Self {
            cluster,
            runtime,
            acknowledged: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl rushdb_storage::Replicator for ClusterReplicator {
    fn replicate(
        &self,
        batch: &rushdb_types::command::CommitBatch,
        payload: &[u8],
    ) -> Result<(), String> {
        let wrapped = ReplicatedCommit {
            batch: batch.clone(),
            payload: payload.to_vec(),
        };
        let cluster = Arc::clone(&self.cluster);
        // Bridge onto the raft runtime from any caller (including a thread
        // that already runs inside a tokio runtime): `Handle::block_on` is
        // forbidden inside a runtime, so hop through a dedicated thread.
        let (tx, rx) = std::sync::mpsc::channel();
        self.runtime.spawn(async move {
            let result = cluster.replicate_batch(&wrapped).await;
            let _ = tx.send(result);
        });
        // Bounded wait: a stalled leader/runtime must not block the committing
        // thread (and the shard lock it holds) indefinitely. The commit key
        // still lets the caller discover the outcome if the write later lands.
        rx.recv_timeout(std::time::Duration::from_secs(30))
            .map_err(|error| match error {
                std::sync::mpsc::RecvTimeoutError::Timeout => {
                    "raft replication timed out after 30s".to_string()
                }
                std::sync::mpsc::RecvTimeoutError::Disconnected => {
                    "raft replication task dropped".to_string()
                }
            })
            .and_then(|res| {
                res.map(|index| {
                    self.acknowledged
                        .store(index, std::sync::atomic::Ordering::Release);
                })
            })
    }

    fn acknowledged_commit_index(&self) -> Option<u64> {
        match self.acknowledged.load(std::sync::atomic::Ordering::Acquire) {
            0 => None,
            index => Some(index),
        }
    }
}

#[derive(Clone)]
struct RushRaftStore {
    inner: Arc<openraft_memstore::MemStore>,
    rushdb: Arc<rushdb_storage::Store>,
}

impl RaftLogReader<TypeConfig> for RushRaftStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        self.inner.try_get_log_entries(range).await
    }
}

impl RaftStorage<TypeConfig> for RushRaftStore {
    type LogReader = Arc<openraft_memstore::MemStore>;
    type SnapshotBuilder = Arc<openraft_memstore::MemStore>;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        self.inner.get_log_state().await
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.save_vote(vote).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.inner.read_vote().await

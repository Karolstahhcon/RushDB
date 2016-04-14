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
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        self.inner.read_committed().await
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, ()>), StorageError<NodeId>> {
        self.inner.last_applied_state().await
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.delete_conflict_logs_since(log_id).await
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.purge_logs_upto(log_id).await
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        self.inner.append_to_log(entries).await
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<ClientResponse>, StorageError<NodeId>> {
        let responses = self.inner.apply_to_state_machine(entries).await?;
        let mut pending: Vec<(LogId<NodeId>, ReplicatedCommit)> = Vec::new();
        for entry in entries {
            if let EntryPayload::Normal(request) = &entry.payload {
                let wrapped: ReplicatedCommit = serde_json::from_str(&request.status)
                    .map_err(|error| StorageIOError::apply(entry.log_id, &error))?;
                pending.push((entry.log_id, wrapped));
            }
        }
        if !pending.is_empty() {
            let first_log_id = pending
                .first()
                .map(|(log_id, _)| *log_id)
                .expect("pending is non-empty");
            let rushdb = Arc::clone(&self.rushdb);
            // The apply path performs a redb fsync. Run it off the async worker
            // so raft heartbeats, elections and RPC handling are not stalled by
            // durability on every committed batch.
            tokio::task::spawn_blocking(move || -> Result<(), StorageError<NodeId>> {
                for (log_id, wrapped) in pending {
                    rushdb
                        .apply_replicated_batch_with_payload(&wrapped.batch, Some(&wrapped.payload))
                        .map_err(|error| StorageIOError::apply(log_id, &error))?;
                }
                Ok(())
            })
            .await
            .map_err(|error| StorageIOError::apply(first_log_id, &error))??;
        }
        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<NodeId>> {
        self.inner.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, ()>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.install_snapshot(meta, snapshot).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        self.inner.get_current_snapshot().await
    }

    fn get_log_reader(&mut self) -> impl std::future::Future<Output = Self::LogReader> {
        std::future::ready(self.inner.clone())
    }

    fn get_snapshot_builder(&mut self) -> impl std::future::Future<Output = Self::SnapshotBuilder> {
        std::future::ready(self.inner.clone())
    }
}

/// Builds in-process [`InProcessNetwork`] clients from a shared node registry;
/// used by deterministic engine tests instead of a TCP transport.
#[derive(Clone, Default)]
pub struct InProcessNetworkFactory {
    nodes: Registry,
}

impl RaftNetworkFactory<TypeConfig> for InProcessNetworkFactory {
    type Network = InProcessNetwork;
    fn new_client(
        &mut self,
        target: NodeId,
        _node: &(),
    ) -> impl std::future::Future<Output = Self::Network> {
        std::future::ready(InProcessNetwork {
            target,
            nodes: self.nodes.clone(),
        })
    }
}

/// Per-target RPC client that forwards `OpenRaft` requests to the target node's
/// in-process handle looked up from the shared registry.
pub struct InProcessNetwork {
    target: NodeId,
    nodes: Registry,
}

impl InProcessNetwork {
    async fn target(&self) -> Result<OpenRaft, RPCError<NodeId, (), RaftError<NodeId>>> {
        self.nodes
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                let error = std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "raft peer unavailable",
                );
                RPCError::Unreachable(Unreachable::new(&error))
            })
    }
}

impl RaftNetwork<TypeConfig> for InProcessNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, (), RaftError<NodeId>>> {
        let node = self.target().await?;
        node.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, (), RaftError<NodeId, InstallSnapshotError>>,
    > {
        let node = self
            .nodes
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                let error = std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "raft peer unavailable",
                );
                RPCError::Unreachable(Unreachable::new(&error))
            })?;
        node.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, (), RaftError<NodeId>>> {
        let node = self.target().await?;
        node.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

/// A fully in-process `OpenRaft` cluster used to exercise consensus semantics
/// in tests, with one memstore log and one `RushDB` state machine per node.
pub struct OpenRaftCluster {
    nodes: Registry,
    /// Retained node handles so a partitioned node can be reconnected (the
    /// registry is the only reachable-copy; dropping a handle from it isolates
    /// the node without losing its state/log).
    handles: Vec<OpenRaft>,
    raft_stores: Vec<Arc<openraft_memstore::MemStore>>,
    state_stores: Vec<Arc<rushdb_storage::Store>>,
}

/// How many `ForwardToLeader` redirects a client write follows.
const FORWARD_HOPS: usize = 5;

/// Bound on waiting for a pending membership change to commit; the wait
/// itself returns as soon as it does.
const MEMBERSHIP_SETTLE_BOUND: Duration = Duration::from_secs(30);

impl OpenRaftCluster {
    /// Create a cluster of `size` reachable nodes, all initialized as voters,
    /// using the given heartbeat and election timeouts.
    ///
    /// # Errors
    ///
    /// Returns an error when `size` is zero, when `OpenRaft` rejects the
    /// configuration, or when node initialization fails.
    pub async fn new(size: usize, heartbeat: Duration, election: Duration) -> Result<Self, String> {
        Self::new_with_local_store(size, heartbeat, election, None).await
    }

    /// A cluster whose initial membership is only node 0; every node is created
    /// and reachable, and additional voters join via `change_membership` (the
    /// P19 catch-up path: a joining node replays the committed log).
    ///
    /// # Errors
    ///
    /// Returns an error if cluster construction succeeds but the follow-up
    /// `change_membership` call cannot be committed.
    pub async fn new_with_expandable_membership(
        size: usize,
        heartbeat: Duration,
        election: Duration,
    ) -> Result<Self, String> {
        let cluster = Self::new_with_local_store(size, heartbeat, election, None).await?;
        cluster.change_membership(&[0]).await?;
        Ok(cluster)
    }

    /// Like `new`, but node 0's `RushDB` state machine is the caller's own
    /// externally-owned store (the HTTP-serving node). Every cluster member
    /// applies every replicated batch, so this store both serves reads and
    /// receives the replicated outcome before `replicate` returns.
    ///
    /// # Errors
    ///
    /// Returns an error when `size` is zero, when `OpenRaft` rejects the
    /// configuration, or when node creation or initialization fails.
    ///
    /// # Panics
    ///
    /// Panics if node 0 is missing from the registry during initialization,
    /// which cannot happen because node 0 is always created first.
    pub async fn new_with_local_store(
        size: usize,
        heartbeat: Duration,
        election: Duration,
        local_store: Option<Arc<rushdb_storage::Store>>,
    ) -> Result<Self, String> {
        if size == 0 {
            return Err("raft cluster requires at least one node".into());
        }
        let registry: Registry = Arc::new(RwLock::new(HashMap::new()));
        let mut handles = Vec::with_capacity(size);
        let mut raft_stores = Vec::with_capacity(size);
        let mut state_stores = Vec::with_capacity(size);
        #[expect(
            clippy::cast_possible_truncation,
            reason = "millisecond durations are far below u64::MAX"
        )]
        let heartbeat_ms = heartbeat.as_millis().max(1) as u64;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "millisecond durations are far below u64::MAX"
        )]
        let election_min_ms = election.as_millis().max(2) as u64;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "millisecond durations are far below u64::MAX"

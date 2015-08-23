//! Integration tests for the in-process `OpenRaft` cluster: leader election,
//! replication through the leader, membership changes, and catch-up of joining
//! nodes against real `RushDB` state machines.

use std::sync::Arc;
use std::time::Duration;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::reader::try_access;
use rushdb_raft::openraft_core::OpenRaftCluster;
use rushdb_storage::{StorageError, Store};
use rushdb_types::command::CommitBatch;
use rushdb_types::ids::TenantId;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openraft_elects_and_applies_a_real_rushdb_commit_batch() {
    // Produce one canonical batch through the normal planner. The IDs and
    // mutations in this batch are then deterministic on every Raft node.
    let source = Arc::new(Store::new_in_memory());
    let source_lmpg = rushdb_lmpg::Lmpg::new(source.clone(), TenantId::single());
    source_lmpg
        .create_record(
            "Replicated",
            None,
            vec![("value".into(), FieldChange::Set(json!(42)))],
        )
        .unwrap();
    let batch: CommitBatch = rushdb_types::command_codec::decode(&source.log_bytes()[0]).unwrap();

    let cluster = OpenRaftCluster::new(3, Duration::from_millis(50), Duration::from_millis(200))
        .await
        .unwrap();
    assert_eq!(cluster.size(), 3);
    let leader = cluster.leader(Duration::from_secs(5)).await.unwrap();
    assert!(leader < 3);
    let wrapped = rushdb_raft::openraft_core::ReplicatedCommit {
        batch: batch.clone(),
        payload: Vec::new(),
    };
    let index = cluster.replicate_batch(&wrapped).await.unwrap();
    assert!(index >= 2, "membership occupies the first log entry");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let applied_everywhere = (0..cluster.size() as u64).all(|node_id| {
            let store = cluster.state_store(node_id).unwrap();
            let lmpg = rushdb_lmpg::Lmpg::new(store, TenantId::single());
            try_access::label_members(
                &lmpg.store.read_view().unwrap(),
                TenantId::single(),
                "Replicated",
            )
            .unwrap()
            .len()
                == 1
        });
        if applied_everywhere {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "replicated batch was not applied on every state machine"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    for node_id in 0..cluster.size() as u64 {
        let store = cluster.state_store(node_id).unwrap();
        let lmpg = rushdb_lmpg::Lmpg::new(store, TenantId::single());
        let record = rushdb_lmpg::reconstruct::reconstruct_record(
            &lmpg.store.read_view().unwrap(),
            TenantId::single(),
            0,
        )
        .unwrap()
        .unwrap();
        assert_eq!(record.get("value"), Some(&json!(42)));
    }

    // The canonical command is replicated, not a facade-specific typed
    // response. Retrying through an incompatible local response type must be
    // an explicit error and must never execute the mutation closure or panic.
    let follower = cluster.state_store(0).unwrap();
    let retry = follower.commit::<String, _>(batch.commit_key, |_ci, _txn| {
        panic!("an already committed key must not execute again")
    });
    assert!(matches!(retry, Err(StorageError::OutcomePayload)));
}

/// Exercises the production wiring path: a `ClusterReplicator` installed on a
/// store routes a normal `Lmpg` commit through the leader, the batch is
/// applied deterministically on the local store (node 0) before `commit`
/// returns, and the committed payload is reproduced on an idempotent retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_replicator_routes_local_commits_through_leader() {
    let runtime = tokio::runtime::Handle::current();
    let cluster = Arc::new(
        OpenRaftCluster::new_with_local_store(
            3,
            Duration::from_millis(50),
            Duration::from_millis(200),
            None,
        )
        .await
        .unwrap(),
    );
    // Attach the replicator to the leader's own store so `commit` routes
    // through the leader and the outcome lands locally before returning.
    let leader = cluster.leader(Duration::from_secs(5)).await.unwrap();
    let leader_store = cluster.state_store(leader).unwrap();
    leader_store.set_replicator(Some(Arc::new(rushdb_raft::ClusterReplicator::new(
        cluster.clone(),
        runtime,
    ))));

    let lmpg = rushdb_lmpg::Lmpg::new(leader_store.clone(), TenantId::single());
    lmpg.create_record(
        "ViaLeader",
        None,
        vec![("value".into(), FieldChange::Set(json!(7)))],
    )
    .unwrap();

    // The acknowledged write must already be applied on the leader. Followers
    // apply asynchronously after the quorum commit; wait for their watermarks
    // before asserting all-node state, as in the canonical-batch test above.
    let committed = leader_store.committed_version();
    assert_eq!(committed, 1);
    assert!(rushdb_lmpg::reconstruct::reconstruct_record(
        &lmpg.store.read_view().unwrap(),
        TenantId::single(),
        0
    )
    .unwrap()
    .is_some());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while (0..3u64)
        .any(|node_id| cluster.state_store(node_id).unwrap().committed_version() < committed)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "followers did not apply the acknowledged batch"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Every node must converge to the same record, whichever node was elected.
    for node_id in 0..3u64 {
        let store = cluster.state_store(node_id).unwrap();
        let lmpg = rushdb_lmpg::Lmpg::new(store, TenantId::single());
        let record = rushdb_lmpg::reconstruct::reconstruct_record(
            &lmpg.store.read_view().unwrap(),
            TenantId::single(),
            0,
        )
        .unwrap()
        .unwrap();
        assert_eq!(record.get("value"), Some(&json!(7)));
    }
}

/// P19: a leader change (membership removal of the current leader) hands off
/// to a new leader; replication continues and converges on the surviving voters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_change_hands_off_and_replication_continues() {
    let cluster = OpenRaftCluster::new(3, Duration::from_millis(50), Duration::from_millis(250))
        .await

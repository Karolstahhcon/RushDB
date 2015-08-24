//! Pinned-policy and generation-isolation gates for replicated authorization:
//! policy state is scoped per tenant, historical views keep the policy pinned
//! at their commit, and malformed policy state fails closed rather than
//! degrading to boundary-only or no-policy behavior.

use rushdb_authz::{
    load_active_policy, load_active_policy_in_view, store_policy, ActiveAuthorizer,
    AuthorizationMode, AuthzError, DataAccessPolicy,
};
use rushdb_lmpg::{Authorizer, BatchOp, Lmpg};
use rushdb_query::SearchRequest;
use rushdb_storage::Store;
use rushdb_tx::{TxCoordinator, TxOutcome};
use rushdb_types::{
    command::{CommitBatch, IsolationMode, Mutation, COMMAND_VERSION},
    ids::TenantId,
    keys::*,
};
use std::{sync::Arc, time::Duration};

fn policy(label: &str) -> DataAccessPolicy {
    DataAccessPolicy {
        allowed_labels: vec![label.into()],
        denied_rel_types: vec!["SECRET".into()],
        ..Default::default()
    }
}

fn on_backends(test: impl Fn(Arc<Store>, TenantId)) {
    let path = rushdb_storage::TempPath::store("policy");
    for disk in [false, true] {
        test(
            Arc::new(if disk {
                Store::open(&path).unwrap()
            } else {
                Store::new_in_memory()
            }),
            TenantId(u32::MAX - 1),
        );
    }
    rushdb_storage::remove_store(&path).unwrap();
}

fn apply(
    store: &Store,
    tenant: TenantId,
    version: u32,
    mutations: Vec<Mutation>,
) -> Result<u64, String> {
    store
        .apply_replicated_batch(&CommitBatch {
            command_version: version,
            tenant,
            transaction_id: "policy-test".into(),
            commit_key: uuid::Uuid::now_v7(),
            start_index: 0,
            isolation: IsolationMode::ReadCommitted,
            preconditions: vec![],
            mutations,
        })
        .map_err(|e| e.to_string())
}

fn coordinator(store: Arc<Store>, tenant: TenantId) -> TxCoordinator {
    TxCoordinator::new_with_authorizer(
        store.clone(),
        tenant,
        Duration::from_secs(30),
        Some(Arc::new(ActiveAuthorizer::new(store, tenant))),
    )
    .unwrap()
}

#[test]
fn tenant_policies_and_transaction_generations_are_isolated_and_pinned() {
    on_backends(|store, tenant| {
        let a = Lmpg::new(store.clone(), tenant);
        let b = Lmpg::new(store.clone(), TenantId(u32::MAX - 2));
        let initial = store_policy(&a, &policy("A"), AuthorizationMode::PolicyEnforced).unwrap();
        let view = store.read_view().unwrap();
        let coord = coordinator(store.clone(), tenant);
        let tx = coord.begin().unwrap();
        coord
            .stage(
                &tx,
                BatchOp::CreateRecord {
                    label: "A".into(),
                    explicit_id: None,
                    fields: vec![],
                },

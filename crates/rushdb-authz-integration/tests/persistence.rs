//! Replicated authorization (plan document 23): policies live under the
//! tenant `security/` keyspace, travel through the deterministic state
//! machine and redb, carry a monotonic generation, and are
//! re-evaluated at commit time against live policy state — an opaque
//! transaction id is never authority.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rushdb_authz::{
    load_active_policy, store_policy, ActiveAuthorizer, AuthorizationMode, DataAccessPolicy,
    SecurityScope,
};
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{BatchOp, Lmpg};
use rushdb_storage::Store;
use rushdb_tx::{TxCoordinator, TxOutcome};
use rushdb_types::ids::TenantId;
use serde_json::json;

fn policy_allow_episode() -> DataAccessPolicy {
    DataAccessPolicy {
        allowed_labels: vec!["Episode".into()],
        denied_rel_types: vec!["SECRET_LINK".into()],
        visible_properties: BTreeMap::default(),
        max_results: 10,
    }
}

fn memory() -> (Arc<Store>, TenantId) {
    (Arc::new(Store::new_in_memory()), TenantId::from_ordinal(71))
}

fn count_label(lmpg: &Lmpg, label: &str) -> usize {
    rushdb_lmpg::reader::try_access::label_members(
        &lmpg.store.read_view().unwrap(),
        lmpg.tenant,
        label,
    )
    .unwrap()
    .len()
}

#[test]
fn store_policy_then_default_deny_enforced_on_mutation() {
    let (store, tenant) = memory();
    let lmpg = Lmpg::new(store.clone(), tenant);
    lmpg.set_authorizer(Arc::new(ActiveAuthorizer::new(store, tenant)));

    let ap = store_policy(
        &lmpg,
        &policy_allow_episode(),
        AuthorizationMode::PolicyEnforced,
    )
    .unwrap();
    assert_eq!(ap.generation, 1);

    // Granted label passes.
    let rid = lmpg
        .create_record(
            "Episode",
            None,
            vec![("title".into(), FieldChange::Set(json!("s1e1")))],
        )
        .unwrap();

    // Denied label is rejected by the engine itself (default-deny, not just
    // boundary filtering).
    let err = lmpg
        .create_record(
            "Note",
            None,
            vec![("body".into(), FieldChange::Set(json!("x")))],
        )
        .unwrap_err();
    assert!(err.to_reason().contains("not authorized"), "{err:?}");

    // The granted record is still queryable through the compiled scope.
    let scope = SecurityScope::compile(&ap.policy, ap.mode);
    let page = rushdb_authz::find_records_authorized(
        &lmpg,
        &rushdb_query::SearchRequest::default(),
        &scope,
    )
    .unwrap();
    assert_eq!(page.total_matched, 1);
    assert_eq!(page.records[0].get("title"), Some(&json!("s1e1")));
    let _ = rid;
}

#[test]
fn denied_rel_type_rejected_on_mutation() {
    let (store, tenant) = memory();
    let lmpg = Lmpg::new(store.clone(), tenant);
    lmpg.set_authorizer(Arc::new(ActiveAuthorizer::new(store, tenant)));
    store_policy(
        &lmpg,
        &policy_allow_episode(),
        AuthorizationMode::PolicyEnforced,
    )
    .unwrap();

    let a = lmpg.create_record("Episode", None, vec![]).unwrap();
    let b = lmpg.create_record("Episode", None, vec![]).unwrap();

    let err = lmpg
        .create_relationship("SECRET_LINK", a, b, vec![])
        .unwrap_err();
    assert!(err.to_reason().contains("denied"), "{err:?}");

    // A non-denied relationship type passes.
    lmpg.create_relationship("SEQUEL", a, b, vec![]).unwrap();
}

#[test]
fn policy_rotation_aborts_inflight_cross_request_transaction() {
    let (store, tenant) = memory();
    let lmpg = Lmpg::new(store.clone(), tenant);
    lmpg.set_authorizer(Arc::new(ActiveAuthorizer::new(store.clone(), tenant)));
    let coord = TxCoordinator::new_with_authorizer(
        store.clone(),
        tenant,
        Duration::from_secs(60),
        Some(Arc::new(ActiveAuthorizer::new(store, tenant))),
    )
    .unwrap();

    let tx = coord.begin().unwrap();
    coord
        .stage(
            &tx,
            BatchOp::CreateRecord {
                label: "Episode".into(),

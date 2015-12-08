use super::*;
use rushdb_lmpg::FieldChange;
use rushdb_storage::resources::StorageResources;
use serde_json::json;

fn store(pool: usize) -> Arc<Store> {
    let store = Arc::new(
        Store::new_with_resources(StorageResources {
            managed_limit_bytes: pool + 128 * 1024,
            page_cache_bytes: 64 * 1024,
            transaction_pool_bytes: pool,
            durability: rushdb_storage::resources::Durability::Local,
            shard_count: 1,
            experimental_multi_shard: false,
            max_group_count: 0,
            max_group_bytes: 0,
            max_group_delay_ms: 0,
            logical_cache_bytes: 0,
            phase_budget_bytes: 0,
            query_memory_max_bytes: 0,
            lock_acquisition_timeout_ms: 0,
            log_segment_bytes: 64 * 1024 * 1024,
            max_expanded_commit_bytes: 512 * 1024 * 1024,
            ..StorageResources::default()
        })
        .unwrap(),
    );
    // Match production engine bootstrap: coordinators do not replace a
    // caller-installed deterministic state-machine expander.
    drop(Lmpg::new(store.clone(), TenantId::from_ordinal(0)));
    store
}
fn limits() -> TransactionLimits {
    TransactionLimits {
        max_active: 2,
        max_operations: 2,
        max_staged_bytes: 64 * 1024,
        max_expired_ids: 1,
    }
}
fn op(size: usize) -> BatchOp {
    BatchOp::CreateRecord {
        label: "Doc".into(),
        explicit_id: None,
        fields: vec![("text".into(), FieldChange::Set(json!("x".repeat(size))))],
    }
}
fn coord(store: &Arc<Store>, tenant: u64, limits: TransactionLimits) -> TxCoordinator {
    TxCoordinator::with_limits(
        store.clone(),
        TenantId::from_ordinal(tenant),
        Duration::from_secs(30),
        None,
        limits,
    )
    .unwrap()
}

#[test]
fn requested_ttl_is_capped_by_the_coordinator_maximum() {
    let store = store(64 * 1024);
    let coordinator = TxCoordinator::with_limits_and_max_ttl(
        store,
        TenantId::from_ordinal(1),
        Duration::from_millis(10),
        Duration::from_millis(20),
        None,
        limits(),
        0,
    )
    .unwrap();

    let tx = coordinator.begin_with(Some(3_600_000), false).unwrap();
    std::thread::sleep(Duration::from_millis(35));
    assert_eq!(
        coordinator.stage(&tx, op(1)).unwrap_err(),
        "transaction expired"
    );
}

#[test]
fn tenants_share_staging_pool_and_failed_stage_is_atomic() {
    let store = store(64 * 1024);
    let a = coord(&store, 1, limits());
    let b = coord(&store, 2, limits());
    let pool = store.transaction_budget();
    let metadata = pool.snapshot().used;
    let first = a.begin().unwrap();
    let second = b.begin().unwrap();
    a.stage(&first, op(40 * 1024)).unwrap();
    let before = pool.snapshot().used;
    assert!(b
        .stage(&second, op(40 * 1024))
        .unwrap_err()
        .contains("resource exhausted"));
    assert_eq!(pool.snapshot().used, before);
    assert!(b.state.lock().unwrap().active[&second].ops.is_empty());
    let outcome = a.commit(&first);
    assert!(matches!(outcome, TxOutcome::Committed(_)), "{outcome:?}");
    b.stage(&second, op(10)).unwrap();
    b.rollback(&second).unwrap();
    assert_eq!(pool.snapshot().used, metadata);
    drop(a);
    drop(b);
    assert_eq!(pool.snapshot().used, 0);
}

/// Tenancy Phase 3: a positive `max_staged_bytes_for_tenant` gives each
/// coordinator its own child of the shared pool, so one tenant's heavy
/// staging is rejected by its own cap — not the shared pool the test above
/// shows a noisy tenant can otherwise exhaust — without affecting a second
/// tenant's concurrent admission.
#[test]
fn per_tenant_staging_cap_isolates_tenants_sharing_one_pool() {
    let store = store(256 * 1024);
    let per_tenant_cap = 40 * 1024;
    // A per-transaction limit well above the per-tenant pool cap, so this
    // test exercises the ByteBudget child, not `TransactionLimits`'s own
    // independent per-transaction byte cap.
    let generous_limits = TransactionLimits {
        max_staged_bytes: 1024 * 1024,
        ..limits()
    };
    let a = TxCoordinator::with_limits_and_max_ttl(
        store.clone(),
        TenantId::from_ordinal(1),
        Duration::from_secs(30),
        Duration::from_secs(30),
        None,
        generous_limits,
        per_tenant_cap,
    )
    .unwrap();
    let b = TxCoordinator::with_limits_and_max_ttl(
        store,
        TenantId::from_ordinal(2),
        Duration::from_secs(30),
        Duration::from_secs(30),
        None,
        generous_limits,
        per_tenant_cap,
    )
    .unwrap();

    let tx_a = a.begin().unwrap();
    a.stage(&tx_a, op(35 * 1024)).unwrap();
    // A second same-sized op exceeds tenant-a's own 40KiB cap, even though
    // the 256KiB store-wide pool still has plenty of room left.
    let err = a.stage(&tx_a, op(35 * 1024)).unwrap_err();
    assert!(err.contains("resource exhausted"), "{err}");
    assert!(
        err.contains("tenant-transaction-staging"),
        "rejection must come from the per-tenant child, not the shared pool: {err}"
    );

    // tenant-b draws from its own child budget and is unaffected by
    // tenant-a's exhaustion.
    let tx_b = b.begin().unwrap();
    b.stage(&tx_b, op(35 * 1024)).unwrap();
}

#[test]
fn active_operation_and_byte_limits_do_not_drop_prior_work() {
    let store = store(64 * 1024);
    let coordinator = coord(
        &store,
        1,
        TransactionLimits {
            max_active: 1,
            max_operations: 1,
            ..limits()
        },
    );
    let tx = coordinator.begin().unwrap();
    assert!(coordinator.begin().is_err());
    assert!(coordinator.stage(&tx, op(70 * 1024)).is_err());
    assert!(coordinator.state.lock().unwrap().active[&tx].ops.is_empty());
    coordinator.stage(&tx, op(1)).unwrap();
    let used = store.transaction_budget().snapshot().used;
    assert!(coordinator.stage(&tx, op(2)).is_err());
    assert_eq!(store.transaction_budget().snapshot().used, used);
    assert_eq!(coordinator.state.lock().unwrap().active[&tx].ops.len(), 1);
    let outcome = coordinator.commit(&tx);
    assert!(matches!(outcome, TxOutcome::Committed(_)), "{outcome:?}");
}

#[test]
fn expiry_releases_payloads_and_retains_only_bounded_recent_ids() {
    let store = store(64 * 1024);
    let coordinator = coord(&store, 1, limits());
    let pool = store.transaction_budget();
    let baseline = pool.snapshot().used;
    let first = coordinator.begin().unwrap();
    coordinator.stage(&first, op(30 * 1024)).unwrap();
    coordinator
        .state
        .lock()
        .unwrap()
        .active
        .get_mut(&first)
        .unwrap()
        .expires_at = Instant::now();
    assert_eq!(coordinator.reap_expired(), 1);
    assert_eq!(coordinator.active_count(), 0);
    assert_eq!(
        pool.snapshot().used,
        baseline,
        "expired tx payload/metadata charges are released on settle"
    );
    assert_eq!(coordinator.commit(&first), TxOutcome::Expired);
    let second = coordinator.begin().unwrap();
    coordinator
        .state
        .lock()
        .unwrap()
        .active
        .get_mut(&second)
        .unwrap()
        .expires_at = Instant::now();
    coordinator.reap_expired();
    assert_eq!(coordinator.commit(&first), TxOutcome::UnknownRetryable);
    assert_eq!(coordinator.commit(&second), TxOutcome::Expired);
    assert_eq!(
        pool.snapshot().used,
        baseline,
        "expired tx charges are released on settle"
    );
    drop(coordinator);
    assert_eq!(pool.snapshot().used, 0);
}

#[test]
fn active_transaction_holds_and_releases_snapshot_retention_pin() {
    let store = store(64 * 1024);
    let coordinator = coord(&store, 1, limits());
    assert_eq!(store.retention_summary(), (None, 0));

    let tx = coordinator.begin().unwrap();
    assert_eq!(store.retention_summary(), (Some(0), 1));

    coordinator.rollback(&tx).unwrap();
    assert_eq!(store.retention_summary(), (None, 0));
}

#[test]
fn compact_encoding_discards_input_spare_capacity_and_checks_depth() {
    let store = store(64 * 1024);
    let coordinator = coord(&store, 1, limits());
    let tx = coordinator.begin().unwrap();
    let baseline = store.transaction_budget().snapshot().used;
    let mut text = String::with_capacity(1024 * 1024);
    text.push('x');
    let operation = BatchOp::CreateRecord {
        label: "Doc".into(),
        explicit_id: None,
        fields: vec![(
            "object".into(),
            FieldChange::Set(serde_json::Value::String(text)),
        )],
    };
    coordinator.stage(&tx, operation).unwrap();
    assert!(store.transaction_budget().snapshot().used - baseline < 1024);
    let mut nested = json!(0);
    for _ in 0..66 {
        nested = json!([nested]);
    }
    let too_deep = BatchOp::CreateRecord {
        label: "Doc".into(),
        explicit_id: None,
        fields: vec![("deep".into(), FieldChange::Set(nested))],
    };
    assert!(coordinator

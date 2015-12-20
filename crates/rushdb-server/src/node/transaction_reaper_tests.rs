use super::metrics::free_percent_ok;
use super::*;

#[test]
fn bounded_json_response_rejects_before_retaining_row() {
    let mut values = Vec::new();
    let mut bytes = 0;
    let error = append_bounded_json_response(
        &mut values,
        &mut bytes,
        serde_json::json!({"payload": "large"}),
        1,
    )
    .expect_err("response byte budget");
    assert!(error.contains("response exceeds configured byte limit"));
    assert!(values.is_empty());
    assert_eq!(bytes, 0);
}

#[test]
fn expired_cursor_releases_resident_bytes() {
    let mut cfg = crate::ServerConfig::default();
    let node = Node::bootstrap(&mut cfg).expect("default node bootstrap");
    let view = Arc::new(node.store.read_view().expect("read view"));
    let cursor = ResultCursor {
        tenant: node.scope().tenant(),
        policy_generation: 0,
        retention_consumer: "cursor:expired".into(),
        view,
        search: SearchRequest::default(),
        next_offset: 0,
        total: 1,
        expires_at: Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("current instant is after one second"),
        _reservation: None,
    };
    let resident = cursor_resident_bytes(&cursor);
    let retention_floor = cursor.view.commit_index();
    node.retain_ephemeral_cursor("cursor:expired".into(), retention_floor)
        .unwrap();
    assert_eq!(node.protected_retention_floor(), Some(retention_floor));
    let cache_budget = node.store.cache_budget();
    let cache_used_before = cache_budget.as_ref().map(|budget| budget.snapshot().used);
    let reservation = cache_budget
        .as_ref()
        .map(|budget| budget.try_reserve(resident).unwrap());
    let cursor = ResultCursor {
        _reservation: reservation,
        ..cursor
    };
    node.cursors
        .lock()
        .unwrap()
        .insert("expired".to_string(), cursor);
    *node.cursor_bytes.lock().unwrap() = resident;

    assert_eq!(node.reap_expired_cursors(), 1);
    assert!(node.cursors.lock().unwrap().is_empty());
    assert_eq!(*node.cursor_bytes.lock().unwrap(), 0);
    assert_eq!(node.protected_retention_floor(), None);
    if let (Some(before), Some(budget)) = (cache_used_before, cache_budget) {
        assert_eq!(budget.snapshot().used, before);
    }
}

#[test]
fn invalid_boot_limits_fail_before_creating_a_directory() {
    let root = rushdb_storage::TempPath::dir("invalid-resource");
    let dir = root.join("data");
    let mut cfg = crate::ServerConfig {
        data_dir: Some(dir.clone()),
        max_request_bytes: 64 * 1024 * 1024,
        listen_addr: "127.0.0.1".into(),
        tls_cert: None,
        tls_key: None,
        ..Default::default()
    };
    cfg.resources.storage_cache = crate::resource_config::ByteSize(1);
    assert!(Node::bootstrap(&mut cfg).is_err());
    assert!(!dir.exists());
}

#[test]
fn restart_applies_saved_cache_before_store_open_and_enforces_tx_limits() {
    use crate::resource_config::ByteSize;
    let dir = rushdb_storage::TempPath::dir("resource-restart");
    let mut cfg = crate::ServerConfig {
        data_dir: Some(dir.to_path_buf()),
        max_request_bytes: 64 * 1024 * 1024,
        listen_addr: "127.0.0.1".into(),
        tls_cert: None,
        tls_key: None,
        ..Default::default()
    };
    cfg.resources.managed_limit = ByteSize(2 * 1024 * 1024);
    cfg.resources.storage_cache = ByteSize(64 * 1024);
    cfg.resources.transaction_pool = ByteSize(64 * 1024);
    cfg.transactions.max_active = 1;
    cfg.transactions.max_operations = 1;
    cfg.transactions.max_staged_bytes = ByteSize(1024);
    cfg.transactions.max_expired_ids = 0;
    let node = Node::bootstrap(&mut cfg).unwrap();
    assert_eq!(node.store.resource_config().page_cache_bytes, 64 * 1024);
    assert_eq!(node.store.resource_budget().snapshot().used, 64 * 1024);
    drop(node);

    let mut restart = crate::ServerConfig {
        data_dir: Some(dir.to_path_buf()),
        max_request_bytes: 64 * 1024 * 1024,
        listen_addr: "127.0.0.1".into(),
        tls_cert: None,
        tls_key: None,
        ..Default::default()
    };

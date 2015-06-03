//! Tenant directory: scope → never-reused 4-byte ordinal.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::{tenancy, Lmpg};
use rushdb_storage::Store;
use rushdb_types::ids::{TenantId, TenantScope};
use serde_json::json;

fn members(store: &Arc<Store>, tenant: TenantId, label: &str) -> usize {
    let view = store.read_view().unwrap();
    try_access::label_members(&view, tenant, label)
        .unwrap()
        .len()
}

#[test]
fn registration_assigns_dense_ordinals_and_is_idempotent() {
    let store = Arc::new(Store::new_in_memory());
    let a = tenancy::resolve_or_register_scope(&store, "project-a").unwrap();
    let b = tenancy::resolve_or_register_scope(&store, "project-b").unwrap();
    assert_eq!(a, TenantId(TenantId::FIRST_ASSIGNED));
    assert_eq!(b, TenantId(TenantId::FIRST_ASSIGNED + 1));
    assert_eq!(
        tenancy::resolve_or_register_scope(&store, "project-a").unwrap(),
        a
    );
    let version = store.committed_version();
    assert_eq!(
        tenancy::resolve_or_register_scope(&store, "project-b").unwrap(),
        b
    );
    assert_eq!(
        store.committed_version(),
        version,
        "resolving a registered scope never commits"
    );
}

#[test]
fn concurrent_registration_of_one_scope_yields_one_ordinal() {
    let store = Arc::new(Store::new_in_memory());
    let mut handles = Vec::new();
    for _ in 0..16 {
        let store = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            tenancy::resolve_or_register_scope(&store, "shared").unwrap()
        }));
    }

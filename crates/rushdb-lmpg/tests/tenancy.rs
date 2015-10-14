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

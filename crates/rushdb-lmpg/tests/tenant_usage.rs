//! USE-1: per-tenant usage counters equal what a full scan of the tenant's
//! rows says, after every commit of a mixed workload, on both backends,
//! across a restart, and through a tenant purge.

use std::collections::BTreeSet;
use std::sync::Arc;

use rushdb_lmpg::derived::{tenant_usage, TenantUsage};
use rushdb_lmpg::import::import_document;
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::{is_usage_bytes_key, Store};
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{self as k, key, P_REL};
use serde_json::json;

/// Usage computed the slow way: scan every row of the tenant.
fn scanned(store: &Store, t: TenantId) -> TenantUsage {
    let view = store.read_view().unwrap();
    let mut usage = TenantUsage::default();
    let mut labels = BTreeSet::new();
    let mut properties = BTreeSet::new();
    let rel_prefix = key(t, &[P_REL, b""]);
    for (row, value) in view.range_prefix(&key(t, &[])).unwrap() {
        if is_usage_bytes_key(&row) {
            continue;
        }
        usage.logical_bytes += (row.len() + value.len()) as u64;
        match row.get(k::TENANT_PREFIX_LEN) {
            Some(&k::F_RECORD_HDR) => {
                usage.records += 1;
                labels.insert(k::decode_record_hdr(&value).unwrap().1.to_owned());
            }
            Some(&k::F_OCC) => {
                let (_, kind, _, pid, _) = k::parse_occ_key(&row).unwrap();
                if kind == OwnerKind::Record {
                    properties.insert(pid);
                }
            }
            _ if row.starts_with(&rel_prefix) => usage.relationships += 1,
            _ => {}
        }
    }
    usage.labels = labels.len() as u64;
    usage.properties = properties.len() as u64;
    usage
}

fn counted(store: &Store, t: TenantId) -> TenantUsage {
    let mut usage = tenant_usage(&store.read_view().unwrap(), t).unwrap();
    usage.vectors.clear();
    usage
}

fn assert_exact(store: &Store, t: TenantId, step: &str) {
    assert_eq!(counted(store, t), scanned(store, t), "after {step}");
}

/// A workload touching every counted figure in both directions, checked
/// after each commit. Two tenants share the store so cross-tenant leakage
/// would show.
fn workload(store: &Arc<Store>) {
    let a = Lmpg::new(Arc::clone(store), TenantId::from_ordinal(11));
    let b = Lmpg::new(Arc::clone(store), TenantId::from_ordinal(12));
    let check = |step: &str| {
        assert_exact(store, a.tenant, step);
        assert_exact(store, b.tenant, step);
    };
    check("nothing");
    let alice = a
        .create_record(
            "Person",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!("Alice"))),
                ("age".into(), FieldChange::Set(json!(31))),
                ("tags".into(), FieldChange::Set(json!(["a", "b"]))),
            ],
        )
        .unwrap();
    check("first record");
    let bob = a
        .create_record(
            "Person",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!("Bob"))),
                ("nick".into(), FieldChange::ExplicitNull),
            ],
        )
        .unwrap();
    let acme = a
        .create_record(

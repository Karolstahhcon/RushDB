//! Behavioral tests for the pinned streaming tenant exporter: view pinning
//! across concurrent writes, bounded-writer streaming, corruption rejection,
//! writer/cancellation failure handling, and cross-reopen stability.

use std::{
    io::{self, Write},
    sync::Arc,
};

use rushdb_backup::{export_tenant_jsonl, export_tenant_jsonl_to};
use rushdb_lmpg::{FieldChange, Lmpg};
use rushdb_storage::{CommitOutcome, Store};
use rushdb_types::{
    command::{CommitBatch, IsolationMode, COMMAND_VERSION},
    ids::TenantId,
};
use serde_json::json;

fn on_backends(test: impl Fn(&Lmpg)) {
    let path = rushdb_storage::TempPath::store("export");
    for disk in [false, true] {
        let store = Arc::new(if disk {
            Store::open(&path).unwrap()
        } else {
            Store::new_in_memory()
        });
        test(&Lmpg::new(store, TenantId(u32::MAX - 1)));
    }
    rushdb_storage::remove_store(&path).unwrap();
}

struct HookWriter<F> {
    bytes: Vec<u8>,
    hook: Option<F>,
}
impl<F: FnOnce()> Write for HookWriter<F> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(hook) = self.hook.take() {
            hook();
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn streaming_export_retains_one_view_across_writes_delete_and_gc() {
    on_backends(|lmpg| {
        let a = lmpg
            .create_record(
                "Doc",
                None,
                vec![("name".into(), FieldChange::Set(json!("before")))],
            )
            .unwrap();
        let b = lmpg.create_record("Doc", None, vec![]).unwrap();
        lmpg.create_relationship(
            "LINK",
            a,
            b,
            vec![("weight".into(), FieldChange::Set(json!(2)))],
        )
        .unwrap();
        let other = Lmpg::new(lmpg.store.clone(), TenantId(u32::MAX - 2));
        other.create_record("OtherTenant", None, vec![]).unwrap();
        let view = lmpg.store.read_view().unwrap();
        let baseline = export_tenant_jsonl(&view, lmpg.tenant).unwrap();
        let mut writer = HookWriter {
            bytes: Vec::new(),
            hook: Some(|| {
                lmpg.update_fields(a, vec![("name".into(), FieldChange::Set(json!("after")))])

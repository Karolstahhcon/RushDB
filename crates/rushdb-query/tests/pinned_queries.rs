//! Pinned-snapshot query coverage: executor reads must resolve against the
//! caller-supplied committed view across in-memory and persistent backends.

use rushdb_lmpg::{import, plan::FieldChange, Lmpg};
use rushdb_query::{
    aggregate, aggregate_with_cancel, find_records, find_records_with_cancel, AggregateRequest,
    CollectSpec, Direction, Expr, FieldPred, QueryError, RelatedExpr, SearchRequest,
};
use rushdb_storage::{StorageError, Store};
use rushdb_types::{
    command::{CommitBatch, IsolationMode, Mutation, COMMAND_VERSION},
    ids::TenantId,
};
use serde_json::json;
use std::{cell::Cell, sync::Arc};

fn on_backends(test: impl Fn(&Lmpg)) {
    let path = rushdb_storage::TempPath::store("pinned-query");
    for persistent in [false, true] {
        let store = Arc::new(if persistent {
            Store::open(&path).unwrap()
        } else {
            Store::new_in_memory()
        });
        let lmpg = Lmpg::new(store, TenantId::from_ordinal(67));
        test(&lmpg);
    }
    rushdb_storage::remove_store(&path).unwrap();
}

#[test]
fn traversal_and_collects_share_root_snapshot_across_writes_and_gc() {
    on_backends(|lmpg| {
        let root = import::import_document(lmpg, "Root", &json!({"name": "before"})).unwrap();
        let child = import::import_document(lmpg, "Child", &json!({"name": "old child"})).unwrap();
        let edge = lmpg
            .create_relationship("HAS", root, child, vec![])
            .unwrap();
        let req = SearchRequest {
            labels: vec!["Root".into()],
            where_: Some(Expr::Related(RelatedExpr {
                target_labels: vec!["Child".into()],
                alias: None,
                rel_type: Some("HAS".into()),
                direction: Direction::Outgoing,
                hops: None,
                cycle: false,
                filter: Some(Box::new(Expr::Field(FieldPred::Eq(
                    "name".into(),
                    json!("old child"),
                )))),
            })),
            collects: vec![CollectSpec {
                steps: Vec::new(),
                alias: "children".into(),
                rel_type: Some("HAS".into()),
                direction: Direction::Outgoing,
                target_labels: vec!["Child".into()],
                fields: vec![("name".into(), "name".into())],
                nested: vec![],
                filter: None,
                order_by: vec![],

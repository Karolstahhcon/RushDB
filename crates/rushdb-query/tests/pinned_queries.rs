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
                skip: 0,
                unique: true,
                limit: None,
            }],
            ..Default::default()
        };
        let before = lmpg.store.committed_version();
        let mutated = Cell::new(false);
        let page = find_records_with_cancel(lmpg, &req, &|| {
            if !mutated.replace(true) {
                lmpg.update_fields(
                    root,
                    vec![("name".into(), FieldChange::Set(json!("after")))],
                )
                .unwrap();
                lmpg.update_fields(
                    child,
                    vec![("name".into(), FieldChange::Set(json!("new child")))],
                )
                .unwrap();
                lmpg.delete_relationship(edge).unwrap();
                import::import_document(lmpg, "Root", &json!({"name": "new root"})).unwrap();
                lmpg.store.gc(u64::MAX).unwrap();
                assert_eq!(lmpg.store.gc_watermark(), before);
            }
            false
        })
        .unwrap();
        assert!(mutated.get());
        assert_eq!(page.total_matched, 1);
        assert_eq!(page.records[0]["name"], json!("before"));
        assert_eq!(page.records[0]["children"], json!([{"name": "old child"}]));
        assert_eq!(find_records(lmpg, &req).unwrap().total_matched, 0);
        lmpg.store.gc(u64::MAX).unwrap();
        assert_eq!(lmpg.store.gc_watermark(), lmpg.store.committed_version());
    });
}

#[test]
fn aggregate_keeps_binding_and_reconstruction_at_one_version() {
    on_backends(|lmpg| {
        let root = import::import_document(lmpg, "Root", &json!({"amount": 10})).unwrap();
        let req = AggregateRequest {
            labels: vec!["Root".into()],
            sums: vec![("total".into(), "amount".into())],
            ..Default::default()
        };
        let mutated = Cell::new(false);
        let result = aggregate_with_cancel(lmpg, &req, &|| {
            if !mutated.replace(true) {
                lmpg.update_fields(root, vec![("amount".into(), FieldChange::Set(json!(99)))])
                    .unwrap();
                lmpg.store.gc(u64::MAX).unwrap();
            }
            false
        })
        .unwrap();
        assert!(mutated.get());
        assert_eq!(result.0[0].sums["total"], json!(10.0));
        assert_eq!(aggregate(lmpg, &req).unwrap()[0].sums["total"], json!(99.0));
    });
}


//! Regressions for the lazy SQL label scan.
//!
//! The scan range is a byte prefix, so it must re-validate each row against the
//! label's exact membership key: a label whose name prefixes another (`user`
//! vs `user_admin`) must not leak rows, and the ordinal suffix must be read at
//! its fixed offset (an ordinal byte equal to `/` must not be mistaken for the
//! separator).

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_sql::execute_sql;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn graph(tenant: u64) -> Lmpg {
    Lmpg::new(
        Arc::new(Store::new_in_memory()),
        TenantId::from_ordinal(tenant),
    )
}

fn count(graph: &Lmpg, sql: &str) -> i64 {
    execute_sql(graph, sql).unwrap()[0]["n"].as_i64().unwrap()
}

#[test]
fn label_prefix_does_not_leak_other_labels() {
    let graph = graph(31);
    for _ in 0..3 {
        graph
            .create_record("user", None, vec![("v".into(), FieldChange::Set(json!(1)))])
            .unwrap();
    }
    for _ in 0..5 {
        graph
            .create_record(
                "user_admin",
                None,
                vec![("v".into(), FieldChange::Set(json!(2)))],
            )
            .unwrap();
    }
    assert_eq!(count(&graph, "SELECT COUNT(*) AS n FROM user"), 3);
    assert_eq!(count(&graph, "SELECT COUNT(*) AS n FROM user_admin"), 5);
    assert_eq!(

//! Integration tests for the `rushdb-sql` read and mutation entry points.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_sql::{
    describe_sql, execute_sql, execute_sql_mutation, execute_sql_mutation_in_view_with_budget,
    execute_sql_with_parameters, execute_sql_with_parameters_budget, SqlError, SqlType,
};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn fixture(store: Arc<Store>, tenant: u64, suffix: &str) -> Lmpg {
    let graph = Lmpg::new(store, TenantId::from_ordinal(tenant));
    let alice = graph
        .create_record(
            "Person",
            None,
            vec![
                (
                    "name".into(),
                    FieldChange::Set(json!(format!("Alice{suffix}"))),
                ),
                ("department".into(), FieldChange::Set(json!("engineering"))),
                ("salary".into(), FieldChange::Set(json!(150_000))),
            ],
        )
        .unwrap();
    let bob = graph
        .create_record(
            "Person",
            None,
            vec![
                (
                    "name".into(),
                    FieldChange::Set(json!(format!("Bob{suffix}"))),
                ),
                ("department".into(), FieldChange::Set(json!("engineering"))),
                ("salary".into(), FieldChange::Set(json!(120_000))),
            ],
        )
        .unwrap();
    let carol = graph
        .create_record(
            "Person",
            None,
            vec![
                (
                    "name".into(),
                    FieldChange::Set(json!(format!("Carol{suffix}"))),
                ),
                ("department".into(), FieldChange::Set(json!("research"))),
                ("salary".into(), FieldChange::Set(json!(170_000))),
            ],
        )
        .unwrap();
    graph
        .create_relationship(
            "KNOWS",
            alice,
            bob,
            vec![("since".into(), FieldChange::Set(json!(2020)))],
        )
        .unwrap();
    graph
        .create_relationship(
            "KNOWS",
            bob,
            carol,
            vec![("since".into(), FieldChange::Set(json!(2022)))],
        )
        .unwrap();
    graph
}

#[test]

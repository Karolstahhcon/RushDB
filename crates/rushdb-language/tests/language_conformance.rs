//! End-to-end conformance tests for the GQL, Cypher, Gremlin, and SQL/PGQ
//! frontends, exercised against a real LMPG tenant.

use std::sync::Arc;

use rushdb_language::{execute, execute_mutation, QueryLanguage};
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
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
                ("age".into(), FieldChange::Set(json!(34))),
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
                ("age".into(), FieldChange::Set(json!(29))),
            ],
        )
        .unwrap();
    graph
        .create_record(
            "Person",
            None,
            vec![
                (
                    "name".into(),
                    FieldChange::Set(json!(format!("Carol{suffix}"))),
                ),
                ("age".into(), FieldChange::Set(json!(22))),
            ],
        )
        .unwrap();
    graph
        .create_relationship(
            "KNOWS",
            alice,
            bob,
            vec![("since".into(), FieldChange::Set(json!(2024)))],
        )
        .unwrap();
    graph
}

#[test]
fn cancelled_language_query_returns_a_cancelled_error() {
    let graph = fixture(Arc::new(Store::new_in_memory()), 77, "cancel");
    let snapshot = Arc::new(graph.store.read_view().unwrap());
    let result = rushdb_language::execute_in_view_with_budget_cancellable(
        &snapshot,
        TenantId::from_ordinal(77),
        QueryLanguage::Cypher,
        "MATCH (p:Person) RETURN p.name AS name",
        None,
        None,
        &|| true,
        None,
    );
    assert!(
        matches!(result, Err(rushdb_language::LanguageError::Cancelled)),
        "expected LanguageError::Cancelled, got {result:?}"
    );
}

#[test]
fn gql_cypher_gremlin_and_sql_pgq_share_one_lmpg_graph() {
    let graph = fixture(Arc::new(Store::new_in_memory()), 1, "");

    let cypher = execute(
        &graph,
        QueryLanguage::Cypher,
        "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name AS source, f.name AS target",
        None,
    )
    .unwrap();
    assert_eq!(
        cypher.rows,
        vec![json!({"source": "Alice", "target": "Bob"})]
    );

    let gql = execute(
        &graph,
        QueryLanguage::Gql,
        "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name AS source, f.name AS target",
        None,
    )
    .unwrap();
    assert_eq!(gql.rows, cypher.rows);

    let gremlin = execute(
        &graph,
        QueryLanguage::Gremlin,
        "g.V().hasLabel('Person').has('age', gt(30)).values('name')",
        None,
    )
    .unwrap();
    assert!(
        gremlin.rows.iter().any(|row| row
            .as_object()
            .unwrap()
            .values()
            .any(|value| value == "Alice")),
        "unexpected Gremlin rows: {:?}",
        gremlin.rows
    );

    let sql = execute(
        &graph,
        QueryLanguage::SqlPgq,
        "SELECT * FROM GRAPH_TABLE (MATCH (p:Person)-[:KNOWS]->(f:Person) COLUMNS (p.name AS source, f.name AS target)) AS g",
        None,
    )
    .unwrap();
    assert_eq!(sql.rows, cypher.rows);
}

#[test]
fn every_language_is_tenant_scoped() {
    let store = Arc::new(Store::new_in_memory());
    let first = fixture(Arc::clone(&store), 10, "-one");
    let second = fixture(store, 20, "-two");

    let cases = [
        (
            QueryLanguage::Cypher,
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
        ),
        (
            QueryLanguage::Gql,
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
        ),
        (
            QueryLanguage::Gremlin,
            "g.V().hasLabel('Person').values('name').order()",
        ),
        (
            QueryLanguage::SqlPgq,
            "SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (n.name AS name)) AS g ORDER BY name",
        ),
    ];

    for (language, statement) in cases {
        let first_rows = execute(&first, language, statement, None).unwrap().rows;
        let first_json = serde_json::to_string(&first_rows).unwrap();
        assert!(first_json.contains("-one"), "{language:?}: {first_json}");
        assert!(!first_json.contains("-two"), "{language:?}: {first_json}");

        let second_rows = execute(&second, language, statement, None).unwrap().rows;
        let second_json = serde_json::to_string(&second_rows).unwrap();
        assert!(second_json.contains("-two"), "{language:?}: {second_json}");
        assert!(!second_json.contains("-one"), "{language:?}: {second_json}");
    }
}

#[test]
fn write_statements_fail_closed_on_the_read_authorized_query_path() {
    let graph = fixture(Arc::new(Store::new_in_memory()), 1, "");
    for (language, statement) in [
        (QueryLanguage::Gql, "INSERT (:Person {name: 'Eve'})"),
        (QueryLanguage::Cypher, "CREATE (:Person {name: 'Eve'})"),
        (
            QueryLanguage::Gremlin,
            "g.addV('Person').property('name', 'Eve')",
        ),
    ] {
        let error = execute(&graph, language, statement, None).unwrap_err();
        let message = error.to_string().to_lowercase();
        assert!(
            message.contains("read-only")
                || message.contains("write")
                || message.contains("mutable"),
            "{language:?} did not fail as read-only: {error}"
        );
    }

    let after = execute(
        &graph,
        QueryLanguage::Cypher,
        "MATCH (n:Person) RETURN count(n) AS count",
        None,
    )
    .unwrap();
    assert_eq!(after.rows, vec![json!({"count": 3})]);
}

#[test]
fn gql_and_cypher_cover_parameters_nulls_aggregation_unwind_and_slicing() {
    let graph = fixture(Arc::new(Store::new_in_memory()), 1, "");

    for language in [QueryLanguage::Gql, QueryLanguage::Cypher] {
        let filtered = execute(
            &graph,
            language,
            "MATCH (p:Person) WHERE p.age >= $minAge RETURN p.name AS name ORDER BY name",
            Some(&json!({"minAge": 30})),
        )
        .unwrap();
        assert_eq!(filtered.rows, vec![json!({"name": "Alice"})]);

        let optional = execute(

use super::*;
use rushdb_lmpg::FieldChange;
use rushdb_storage::{ReadLimits, Store};
use rushdb_types::command::{CommitBatch, IsolationMode, Mutation, COMMAND_VERSION};
use serde_json::json;

#[test]
fn mutation_byte_limit_rejects_fat_properties_and_edges_without_committing() {
    let graph = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::from_ordinal(69));
    let snapshot = Arc::new(graph.store.read_view().unwrap());
    let params = json!({"payload": "x".repeat(8192)});
    let error = execute_mutation_in_view_with_limits(
        &graph,
        &snapshot,
        QueryLanguage::Cypher,
        "CREATE (a:Person {payload: $payload}) RETURN a",
        Some(&params),
        100,
        4096,
        &|| false,
        None,
    )
    .unwrap_err();
    assert!(
        matches!(error, LanguageError::MutationBytesExceeded(4096)),
        "{error}"
    );
    assert_eq!(graph.store.committed_version(), 0);

    execute_mutation(
        &graph,
        QueryLanguage::Cypher,
        "CREATE (:Person {name: 'a'})",
        None,
    )
    .unwrap();
    let before = graph.store.committed_version();
    let snapshot = Arc::new(graph.store.read_view().unwrap());
    let error = execute_mutation_in_view_with_limits(
        &graph,
        &snapshot,
        QueryLanguage::Cypher,
        "MATCH (a:Person) CREATE (a)-[:KNOWS {payload: $payload}]->(a)",
        Some(&params),
        100,
        4096,
        &|| false,
        None,
    )
    .unwrap_err();
    assert!(
        matches!(error, LanguageError::MutationBytesExceeded(4096)),
        "{error}"
    );
    assert_eq!(graph.store.committed_version(), before);
}

#[test]
fn mutation_byte_limit_counts_only_what_the_statement_touches() {
    let graph = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::from_ordinal(69));
    graph
        .create_record(
            "Person",
            None,
            vec![("payload".into(), FieldChange::Set(json!("x".repeat(8192))))],
        )
        .unwrap();
    // The fat record is not copied for a write that does not touch it.
    let snapshot = Arc::new(graph.store.read_view().unwrap());
    execute_mutation_in_view_with_limits(
        &graph,
        &snapshot,
        QueryLanguage::Cypher,
        "CREATE (:Other)",
        None,
        100,
        4096,
        &|| false,
        None,
    )
    .unwrap();
    // A write that changes it holds it, and is refused over the limit.
    let snapshot = Arc::new(graph.store.read_view().unwrap());
    let error = execute_mutation_in_view_with_limits(
        &graph,
        &snapshot,
        QueryLanguage::Cypher,
        "MATCH (p:Person) SET p.flag = true",
        None,
        100,
        4096,
        &|| false,
        None,
    )
    .unwrap_err();
    assert!(matches!(error, LanguageError::MutationBytesExceeded(4096)));
    assert_eq!(graph.store.committed_version(), snapshot.commit_index());
}

fn on_backends(test: impl Fn(&Lmpg)) {
    let path = rushdb_storage::TempPath::store("grafeo-pins");
    for persistent in [false, true] {
        let store = if persistent {
            Store::open(&path).unwrap()
        } else {
            Store::new_in_memory()
        };
        test(&Lmpg::new(Arc::new(store), TenantId::from_ordinal(69)));
    }
    rushdb_storage::remove_store(&path).unwrap();
}

fn fixture(graph: &Lmpg) -> (rushdb_lmpg::RecordRef, rushdb_lmpg::RelationshipRef) {
    let source = graph
        .create_record(
            "Person",
            None,
            vec![("name".into(), FieldChange::Set(json!("before")))],
        )
        .unwrap();
    let target = graph
        .create_record(
            "Person",
            None,
            vec![("name".into(), FieldChange::Set(json!("target")))],
        )
        .unwrap();
    let edge = graph
        .create_relationship(
            "KNOWS",
            source,
            target,
            vec![("since".into(), FieldChange::Set(json!(2001)))],
        )
        .unwrap();
    (source, edge)
}

#[test]
fn pinned_adapter_and_result_reconstruction_survive_writes_deletion_and_gc() {
    on_backends(|graph| {
        let (source, edge) = fixture(graph);
        let view = Arc::new(graph.store.read_view().unwrap());
        let adapter =
            LmpgGraphStore::new(Arc::new(OverlayView::new(view.clone())), graph.tenant, None);
        graph
            .update_fields(
                source,
                vec![("name".into(), FieldChange::Set(json!("after")))],
            )
            .unwrap();
        graph.delete_relationship(edge).unwrap();
        graph.store.gc(u64::MAX).unwrap();
        assert_eq!(
            adapter.current_epoch(),
            grafeo_common::types::EpochId::new(view.commit_index())
        );
        assert_eq!(adapter.out_degree(NodeId::new(source.ordinal)), 1);
        adapter.check().unwrap();
        for language in [QueryLanguage::Cypher, QueryLanguage::Gql] {
            let result = execute_in_view(
                &view,
                graph.tenant,
                language,
                "MATCH (p:Person)-[e:KNOWS]->(f:Person) RETURN p, e",
                None,
            )
            .unwrap();
            assert_eq!(result.rows.len(), 1);

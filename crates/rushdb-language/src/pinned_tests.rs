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
            assert_eq!(result.rows[0]["p"]["name"], json!("before"));
            assert_eq!(result.rows[0]["e"]["since"], json!(2001));
        }
        for (language, query) in [
            (QueryLanguage::Gremlin, "g.V().hasLabel('Person').has('name', 'before').values('name')"),
            (QueryLanguage::SqlPgq, "SELECT * FROM GRAPH_TABLE (MATCH (p:Person)-[:KNOWS]->(f:Person) COLUMNS (p.name AS source)) AS g"),
        ] {
            let result = execute_in_view(&view, graph.tenant, language, query, None).unwrap();
            assert_eq!(result.rows.len(), 1);
            assert!(result.rows[0].as_object().unwrap().values().any(|value| value == "before"));
        }
        assert_eq!(
            execute(
                graph,
                QueryLanguage::Cypher,
                "MATCH (p:Person)-[:KNOWS]->(f) RETURN p",
                None
            )
            .unwrap()
            .rows
            .len(),
            0
        );
    });
}

#[test]
fn graph_result_budget_rejects_before_retaining_rows() {
    on_backends(|graph| {
        fixture(graph);
        let view = Arc::new(graph.store.read_view().unwrap());
        let error = execute_in_view_with_budget(
            &view,
            graph.tenant,
            QueryLanguage::Cypher,
            "MATCH (p:Person) RETURN p",
            None,
            Some(1),
        )
        .expect_err("tiny graph result budget");
        assert!(error.to_string().contains("execution byte budget"));
    });
}

#[test]
fn graph_mutation_in_view_does_not_observe_post_pin_records() {
    on_backends(|graph| {
        fixture(graph);
        let view = Arc::new(graph.store.read_view().unwrap());
        graph
            .create_record(
                "AfterPin",
                None,
                vec![("name".into(), FieldChange::Set(json!("late")))],
            )
            .unwrap();
        // The pinned view has no `AfterPin` record: nothing matches, so the
        // write changes nothing and commits nothing.
        let before = graph.store.committed_version();
        let result = execute_mutation_in_view_with_budget(
            graph,
            &view,
            QueryLanguage::Cypher,
            "MATCH (p:AfterPin) SET p.name='changed'",
            None,
            MUTATION_MAX_NODES,
        )
        .unwrap();
        assert_eq!(result.affected, 0);
        assert_eq!(result.commit_index, view.commit_index());
        assert_eq!(graph.store.committed_version(), before);
    });
}

#[test]
fn graph_mutation_reads_one_pinned_input_view() {
    on_backends(|graph| {
        let (source, edge) = fixture(graph);
        let view = Arc::new(graph.store.read_view().unwrap());
        graph
            .update_fields(
                source,
                vec![("name".into(), FieldChange::Set(json!("after")))],
            )
            .unwrap();
        graph.delete_relationship(edge).unwrap();
        graph.store.gc(u64::MAX).unwrap();
        // Staged against the pinned view: the record still has its old name
        // and the relationship still exists there.
        let staged = stage_statement_mutation_in_overlay(
            graph,
            QueryLanguage::Cypher,
            "MATCH (p:Person {name: 'before'})-[e:KNOWS]->(:Person) SET e.seen = true",
            None,
            MUTATION_MAX_NODES,
            &Arc::new(OverlayView::new(view)),
            None,
        )
        .unwrap();
        assert!(
            matches!(
                staged.ops.as_slice(),
                [rushdb_lmpg::BatchOp::UpdateFields {
                    owner: rushdb_types::ids::OwnerKind::Relationship,
                    owner_ord,
                    ..
                }] if *owner_ord == edge.ordinal
            ),
            "{} ops",
            staged.ops.len()
        );
    });
}

fn corrupt(graph: &Lmpg, storage_key: Vec<u8>) {
    // Malformed base rows are refused by apply-time derivation, so plant them
    // with derivation suspended to prove readers fail on them.
    let commit_key = uuid::Uuid::now_v7();
    graph.store.inject_without_derivation(|| {
        graph
            .store
            .commit(commit_key, |_, _| {
                Ok((
                    CommitBatch {
                        command_version: COMMAND_VERSION,
                        tenant: graph.tenant,
                        transaction_id: String::new(),
                        commit_key,
                        start_index: 0,
                        isolation: IsolationMode::ReadCommitted,
                        preconditions: vec![],
                        mutations: vec![Mutation::PutRawKey {
                            key: storage_key,
                            value: vec![1],
                        }],
                    },
                    (),
                ))
            })
            .unwrap()
    });
}

#[test]
fn corrupt_reads_fail_every_language_instead_of_returning_partial_results() {
    on_backends(|graph| {
        let (source, _) = fixture(graph);
        corrupt(
            graph,
            rushdb_types::keys::record_hdr_key(graph.tenant, source.ordinal),
        );
        for (language, query) in [
            (
                QueryLanguage::Cypher,
                "MATCH (p:Person) RETURN p.name AS name",
            ),
            (QueryLanguage::Gql, "MATCH (p:Person) RETURN p.name AS name"),
            (
                QueryLanguage::Gremlin,
                "g.V().hasLabel('Person').values('name')",
            ),
            (
                QueryLanguage::SqlPgq,
                "SELECT * FROM GRAPH_TABLE (MATCH (p:Person) COLUMNS (p.name AS name)) AS g",
            ),
        ] {
            assert!(
                matches!(
                    execute(graph, language, query, None),

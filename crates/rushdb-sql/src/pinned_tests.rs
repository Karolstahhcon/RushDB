use super::*;
use rushdb_lmpg::{BatchOp, BatchTx, FieldChange, RecordRef};
use rushdb_storage::Store;
use rushdb_types::command::{CommitBatch, IsolationMode, Mutation, COMMAND_VERSION};
use rushdb_types::ids::TenantId;
use serde_json::json;

#[test]
fn synchronous_sql_rejects_runtime_threads_without_writing() {
    let graph = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::from_ordinal(68));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        assert!(matches!(
            execute_sql(&graph, "SELECT 1"),
            Err(SqlError::BlockingInRuntime)
        ));
        assert!(matches!(
            describe_sql(&graph, "SELECT 1"),
            Err(SqlError::BlockingInRuntime)
        ));
        assert!(matches!(
            execute_sql_mutation(&graph, "DELETE FROM person", None),
            Err(SqlError::BlockingInRuntime)
        ));
        assert_eq!(graph.store.committed_version(), 0);
        let rows = execute_sql_async(&graph, "SELECT 1 AS n", None)
            .await
            .unwrap();
        assert_eq!(rows[0]["n"], json!(1));
    });
}

#[test]
fn only_filters_the_scan_answers_in_full_are_exact() {
    // Contract: DataFusion drops an `Exact` filter, so only comparisons whose
    // native predicate keeps exactly SQL's rows are `Exact` (`=` on a column
    // of one kind of value, `>=`/`<=` on a numeric one); anything else is
    // `Inexact` and DataFusion re-applies it.
    use datafusion::datasource::TableProvider;
    use datafusion_expr::TableProviderFilterPushDown::{Exact, Inexact};
    use datafusion_expr::{col, lit};

    let graph = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::from_ordinal(68));
    fixture(&graph);
    graph
        .create_record(
            "Person",
            None,
            vec![
                ("age".into(), FieldChange::Set(json!(30))),
                ("code".into(), FieldChange::Set(json!(1))),
            ],
        )
        .unwrap();
    graph
        .create_record(
            "Person",
            None,
            vec![("code".into(), FieldChange::Set(json!("one")))],
        )
        .unwrap();
    let snapshot = Arc::new(graph.store.read_view().unwrap());
    let table = crate::scan_provider::LabelTable::try_new(
        Arc::new(Lmpg::new(Arc::clone(&graph.store), graph.tenant)),
        "Person".into(),
        snapshot,
    )
    .unwrap();
    let filters = [
        col("name").eq(lit("before")),
        col("age")
            .gt_eq(lit(18i64))
            .and(col("age").lt_eq(lit(65i64))),
        col("age").gt(lit(18i64)),
        col("name").gt(lit("b")),
        col("code").eq(lit("one")),
        col("name").eq(lit("a")).or(col("name").eq(lit("b"))),
        col("__id").eq(lit("x")),
    ];
    let refs: Vec<&datafusion_expr::Expr> = filters.iter().collect();
    assert_eq!(
        table.supports_filters_pushdown(&refs).unwrap(),
        [Exact, Exact, Inexact, Inexact, Inexact, Inexact, Inexact]
    );
}

fn on_backends(test: impl Fn(&Lmpg)) {
    let path = rushdb_storage::TempPath::store("sql-pins");
    for persistent in [false, true] {
        let store = if persistent {
            Store::open(&path).unwrap()
        } else {
            Store::new_in_memory()
        };
        let graph = Lmpg::new(Arc::new(store), TenantId::from_ordinal(68));
        test(&graph);
    }
    rushdb_storage::remove_store(&path).unwrap();
}

fn fixture(graph: &Lmpg) -> (RecordRef, RecordRef, rushdb_lmpg::RelationshipRef) {
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
    (source, target, edge)
}

fn query(context: &SessionContext, sql: &str) -> Vec<Row> {
    futures::executor::block_on(async {
        let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
        batches_to_rows(&batches).unwrap()
    })
}

fn assert_corrupt(error: &SqlError, context: &str) {
    let text = error.to_string().to_lowercase();
    assert!(
        text.contains("corrupt"),
        "{context}: expected a corruption error, got {error:?}"
    );
}

#[test]
fn record_and_relationship_tables_retain_one_version_across_writes_and_gc() {
    on_backends(|graph| {
        let (source, _, edge) = fixture(graph);
        let snapshot = graph.store.read_view().unwrap();
        graph
            .update_fields(
                source,
                vec![("name".into(), FieldChange::Set(json!("after")))],
            )
            .unwrap();
        graph.delete_relationship(edge).unwrap();
        graph.store.gc(u64::MAX).unwrap();
        assert_eq!(graph.store.gc_watermark(), snapshot.commit_index());
        // Both table registrations occur after the concurrent changes, but
        // must use the physical/logical version pinned before them.
        let _ci = snapshot.commit_index();
        let context = context_in_view(graph, &Arc::new(snapshot), None).unwrap();
        let rows = query(&context.context, "SELECT p.name, e.since FROM person p JOIN rushdb_edges.knows e ON p.__id=e.__source_id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], json!("before"));
        assert_eq!(rows[0]["since"], json!(2001));
        assert!(virtual_catalog(graph)
            .unwrap()
            .relationship_tables
            .is_empty());
        assert_eq!(
            execute_sql(graph, "SELECT name FROM person WHERE name='after'")
                .unwrap()
                .len(),
            1
        );

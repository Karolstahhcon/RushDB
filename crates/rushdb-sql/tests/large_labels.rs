//! SQL-2: SQL statements on a label with millions of records. A label
//! table's schema comes from the property catalog, its rows stream, and a
//! filtered scan reads its candidates the way records reads do: from the
//! index plan when an index answers a condition, from a walk of the label
//! otherwise, refused when an unindexed filter would check more records than
//! the unindexed-walk limit. The request's own bounds reach the scan.

use std::sync::Arc;

use rushdb_lmpg::index::IndexStep;
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{Lmpg, RequestLimits};
use rushdb_sql::{
    execute_sql_with_parameters_budget_in_view, reset_work_counters, take_work_counters, SqlError,
};
use rushdb_storage::resources::StorageResources;
use rushdb_storage::{ReadView, Store};
use rushdb_types::ids::TenantId;
use rushdb_types::index::IndexKind;
use serde_json::json;

const RECORDS: i64 = 300;

/// `Big` records: `n` = i mod 10, `code` an integer except one text.
fn engine(walk_limit: u64) -> Lmpg {
    engine_of(walk_limit, RECORDS)
}

fn engine_of(walk_limit: u64, records: i64) -> Lmpg {
    let store = Store::new_with_resources(StorageResources {
        unindexed_walk_max_owners: walk_limit,
        ..StorageResources::default()
    })
    .unwrap();
    let lmpg = Lmpg::new(Arc::new(store), TenantId::single());
    for i in 0..records {
        let code = if i == 7 { json!("x") } else { json!(i) };
        lmpg.create_record(
            "Big",
            None,
            vec![
                ("n".into(), FieldChange::Set(json!(i % 10))),
                ("code".into(), FieldChange::Set(code)),
            ],
        )
        .unwrap();
    }
    lmpg
}

fn index(lmpg: &Lmpg, property: &str) {
    let def = lmpg
        .define_index(
            &format!("big_{property}"),
            "Big",
            property,
            IndexKind::Ordered,
        )
        .unwrap();
    while !matches!(
        lmpg.index_step(def.id, 1_000).unwrap(),
        IndexStep::Finished | IndexStep::Idle
    ) {}
}

fn sql_in(lmpg: &Lmpg, view: Arc<ReadView>, sql: &str) -> Result<Vec<serde_json::Value>, SqlError> {
    execute_sql_with_parameters_budget_in_view(lmpg, view, sql, None, None).map(|rows| {
        rows.into_iter()
            .map(|row| serde_json::Value::Object(row.into_iter().collect()))
            .collect()
    })
}

fn sql(lmpg: &Lmpg, sql: &str) -> Result<Vec<serde_json::Value>, SqlError> {
    sql_in(lmpg, Arc::new(lmpg.store.read_view().unwrap()), sql)
}

#[test]
fn a_label_past_the_materialization_ceiling_streams() {
    // At 10M records every statement failed: the table listed its label's
    // members, and rebuilt every one to infer its schema, through a scan
    // capped at 64 MiB (`scan row exceeds batch byte limit`). Here the
    // ceiling is 16 KiB, under the 3,000 records' member keys.
    let lmpg = engine_of(0, 3_000);
    index(&lmpg, "n");
    let view = Arc::new(lmpg.store.read_view().unwrap()).with_materialization_limit(16 * 1024);
    assert!(
        view.range_prefix(&rushdb_types::keys::label_member_prefix(lmpg.tenant, "Big"))
            .is_err(),
        "the ceiling must bind a collected scan"
    );
    let count = |sql: &str| sql_in(&lmpg, Arc::clone(&view), sql).unwrap();
    // Planned on the index.
    assert_eq!(
        count("SELECT count(*) AS c FROM big WHERE n = 5"),
        vec![json!({"c": 300})]
    );
    // Walked: no condition the scan can state.
    assert_eq!(
        count("SELECT count(*) AS c FROM big WHERE n + 1 = 6"),
        vec![json!({"c": 300})]
    );
    assert_eq!(count("SELECT n FROM big LIMIT 3").len(), 3);
}

#[test]
fn an_indexed_condition_reads_only_its_candidates() {
    let lmpg = engine(0);
    index(&lmpg, "n");
    reset_work_counters();
    let rows = sql(&lmpg, "SELECT count(*) AS c FROM big WHERE n = 5").unwrap();
    assert_eq!(rows, vec![json!({"c": 30})]);
    assert_eq!(take_work_counters().candidate_rows, 30);

    // A condition the scan cannot state leaves the indexed one planned.
    reset_work_counters();
    let rows = sql(
        &lmpg,
        "SELECT count(*) AS c FROM big WHERE n = 5 AND (code = 5 OR code = 15)",
    )
    .unwrap();
    assert_eq!(rows, vec![json!({"c": 2})]);
    assert_eq!(take_work_counters().candidate_rows, 30);

    // Ranges widen to inclusive bounds and are re-checked exactly.
    let rows = sql(&lmpg, "SELECT count(*) AS c FROM big WHERE n > 7").unwrap();
    assert_eq!(rows, vec![json!({"c": 60})]);

    // `__id` is one id lookup.
    let id = sql(&lmpg, "SELECT __id FROM big WHERE n = 3 LIMIT 1").unwrap()[0]["__id"]
        .as_str()
        .unwrap()
        .to_owned();
    reset_work_counters();
    let rows = sql(&lmpg, &format!("SELECT n FROM big WHERE __id = '{id}'")).unwrap();
    assert_eq!(rows, vec![json!({"n": 3})]);
    assert_eq!(take_work_counters().candidate_rows, 1);
}

#[test]
fn a_text_comparison_on_a_widened_column_keeps_every_matching_row() {
    // `code` holds integers and one text value, so its column is text and
    // `code = '5'` matches the integer 5. An index probe for the text "5"

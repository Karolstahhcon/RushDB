//! SQL-3: `INSERT`/`UPDATE`/`DELETE` on a tenant past the old 100,000-row
//! materialization ceiling. DML reads its rows the way `SELECT` does (streamed
//! label tables) and commits only the touched records.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{BatchOp, BatchTx, Lmpg};
use rushdb_sql::{
    execute_sql, execute_sql_mutation, execute_sql_mutation_in_view_with_budget, SqlError,
    MAX_SCAN_ROWS,
};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

/// One record past the old ceiling, in a label that also has a small sibling.
const RECORDS: usize = MAX_SCAN_ROWS + 1;

fn engine() -> Lmpg {
    let lmpg = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::single());
    for chunk in (0..RECORDS).collect::<Vec<_>>().chunks(10_000) {
        let ops = chunk
            .iter()
            .map(|&i| BatchOp::CreateRecord {
                label: "Big".into(),
                explicit_id: None,
                fields: vec![
                    ("n".into(), FieldChange::Set(json!(i % 10))),
                    ("Code Name".into(), FieldChange::Set(json!(format!("c{i}")))),
                ],
            })
            .collect();
        lmpg.run_batch(BatchTx { ops }).unwrap();
    }
    lmpg.create_record(
        "Small",
        None,
        vec![("n".into(), FieldChange::Set(json!(3)))],
    )
    .unwrap();
    lmpg
}

fn count(lmpg: &Lmpg, sql: &str) -> u64 {
    execute_sql(lmpg, sql).unwrap()[0]["c"].as_u64().unwrap()
}

#[test]
fn dml_on_a_label_past_the_old_ceiling() {
    let lmpg = engine();
    let tenth = (RECORDS / 10) as u64; // rows with n = 3 (RECORDS - 1 ends in 0)

    // UPDATE by predicate, with a parameter and a quoted mixed-case column.
    let updated = execute_sql_mutation(
        &lmpg,
        "UPDATE big SET \"Code Name\" = 'hit' WHERE n = $1",
        Some(&json!([3])),
    )
    .unwrap();
    assert_eq!(updated.affected, tenth);
    assert_eq!(
        count(
            &lmpg,
            "SELECT COUNT(*) AS c FROM big WHERE \"Code Name\" = 'hit'"
        ),
        tenth
    );

    // An UPDATE touching every row, the whole label in one commit.
    let all = execute_sql_mutation(&lmpg, "UPDATE big SET n = n + 10", None).unwrap();
    assert_eq!(all.affected, RECORDS as u64);
    assert_eq!(
        count(&lmpg, "SELECT COUNT(*) AS c FROM big WHERE n < 10"),
        0
    );

    // A row that already holds the value is matched but not rewritten.

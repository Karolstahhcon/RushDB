//! SQL-4: `rushdb_edges` tables past the old 100,000-relationship ceiling.
//! Edge tables stream from the relationship index instead of being built in
//! memory.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{BatchOp, BatchTx, Lmpg};
use rushdb_sql::{execute_sql, MAX_SCAN_ROWS};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

const PEOPLE: usize = 1_000;
const KNOWS: usize = MAX_SCAN_ROWS + 1;

#[test]
fn edge_tables_stream_past_the_old_ceiling() {
    let lmpg = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::single());
    let people = lmpg
        .run_batch(BatchTx {
            ops: (0..PEOPLE)
                .map(|i| BatchOp::CreateRecord {
                    label: "Person".into(),
                    explicit_id: None,
                    fields: vec![("n".into(), FieldChange::Set(json!(i)))],
                })
                .collect(),
        })
        .unwrap()
        .created_records;
    for chunk in (0..KNOWS).collect::<Vec<_>>().chunks(10_000) {
        let ops = chunk
            .iter()
            .map(|&i| BatchOp::CreateRelationship {
                rel_type: "KNOWS".into(),
                source: people[i % PEOPLE],
                target: people[(i * 7 + 1) % PEOPLE],
                fields: vec![("weight".into(), FieldChange::Set(json!(i % 10)))],
            })
            .collect();
        lmpg.run_batch(BatchTx { ops }).unwrap();
    }
    lmpg.run_batch(BatchTx {
        ops: vec![BatchOp::CreateRelationship {
            rel_type: "LIKES".into(),
            source: people[0],
            target: people[1],
            fields: Vec::new(),
        }],
    })
    .unwrap();
    let count = |sql: &str| execute_sql(&lmpg, sql).unwrap()[0]["c"].as_u64().unwrap();
    assert_eq!(
        count("SELECT COUNT(*) AS c FROM rushdb_edges.knows"),
        KNOWS as u64
    );
    assert_eq!(
        count("SELECT COUNT(*) AS c FROM rushdb_edges.knows WHERE weight = 3"),
        (KNOWS / 10) as u64
    );
    assert_eq!(count("SELECT COUNT(*) AS c FROM rushdb_edges.likes"), 1);
    // Joined with the endpoint table.
    assert_eq!(
        count(
            "SELECT COUNT(*) AS c FROM rushdb_edges.knows k \
             JOIN person p ON p.__id = k.__source_id WHERE p.n = 0"
        ),
        (KNOWS / PEOPLE + 1) as u64
    );
}

/// An edge table's columns and row count come from the relationship catalog
/// without reading a relationship: every property the type's relationships
/// carry is a column of its stored type (arrays as lists, int and float
/// together as float), only the type's own properties are columns, and the
/// rows decode into them.
#[test]
fn edge_columns_come_from_the_catalog() {
    let lmpg = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::single());
    let people = lmpg
        .run_batch(BatchTx {
            ops: (0..2)
                .map(|i| BatchOp::CreateRecord {
                    label: "Person".into(),
                    explicit_id: None,

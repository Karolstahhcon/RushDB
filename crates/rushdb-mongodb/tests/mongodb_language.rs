//! Integration tests for the MongoDB query-language lowering.

use std::sync::Arc;

use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    let lmpg = Lmpg::new(store, TenantId::from_ordinal(9001));
    for name in ["Jedi Order", "Sith Order", "Galactic Republic"] {
        lmpg.create_record(
            "FACTION",
            None,
            vec![("name".into(), rushdb_lmpg::FieldChange::Set(json!(name)))],
        )
        .unwrap();
    }
    lmpg
}

#[test]
fn find_translates_filter_projection_sort_and_pagination() {
    let lmpg = setup();
    let (total, rows) = rushdb_mongodb::execute_query(
        &lmpg,
        &json!({
            "collection": "FACTION",
            "filter": { "name": { "$ne": "Sith Order" } },
            "projection": { "name": 1 },
            "sort": { "name": 1 },
        })
        .to_string(),
        &|| false,
    )
    .unwrap();
    assert_eq!(total, 2);
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows
        .iter()
        .map(|r| r.get("name").and_then(|v| v.as_str()).unwrap_or(""))
        .collect();
    assert_eq!(names, vec!["Galactic Republic", "Jedi Order"]);
}

#[test]
fn invalid_sort_direction_is_rejected_instead_of_defaulting_ascending() {
    let lmpg = setup();
    let error = rushdb_mongodb::execute_query(
        &lmpg,
        &json!({ "collection": "FACTION", "sort": { "name": 0 } }).to_string(),
        &|| false,
    )
    .unwrap_err();
    assert!(error.contains("sort direction must be 1 or -1"));
}

#[test]
fn unsupported_projection_forms_are_rejected() {
    let lmpg = setup();
    for projection in [
        json!({ "name": 0 }),
        json!({ "name": false }),
        json!({ "name": 2 }),
    ] {
        let error = rushdb_mongodb::execute_query(
            &lmpg,
            &json!({ "collection": "FACTION", "projection": projection }).to_string(),
            &|| false,
        )
        .unwrap_err();
        assert!(error.contains("projection"));
    }
}

#[test]
fn aggregate_group_sort_limit_parity() {
    let lmpg = setup();
    let (total, rows) = rushdb_mongodb::execute_query(
        &lmpg,
        &json!({
            "aggregate": [
                { "$match": {} },
                { "$group": { "_id": "$name", "count": { "$sum": 1 } } },
                { "$sort": { "count": -1 } },
                { "$limit": 2 }
            ],
            "collection": "FACTION"
        })
        .to_string(),
        &|| false,
    )
    .unwrap();
    assert_eq!(total, 2);
    assert!(rows.iter().all(|r| r.get("count").is_some()));
}

#[test]
fn insert_update_delete_commands_affect_state() {
    let lmpg = setup();
    // insertOne
    let affected = rushdb_mongodb::execute_mutation(
        &lmpg,
        &json!({ "insertOne": { "collection": "FACTION", "document": { "name": "Rebel Alliance" } } }).to_string(),
    )
    .unwrap();
    assert_eq!(affected, 1);

    // updateOne with $set
    let affected = rushdb_mongodb::execute_mutation(
        &lmpg,
        &json!({
            "updateOne": {
                "collection": "FACTION",
                "filter": { "name": "Rebel Alliance" },
                "update": { "$set": { "name": "Alliance to Restore the Republic" } }
            }
        })
        .to_string(),
    )
    .unwrap();
    assert_eq!(affected, 1);

    // deleteOne
    let affected = rushdb_mongodb::execute_mutation(
        &lmpg,
        &json!({ "deleteOne": { "collection": "FACTION", "filter": { "name": "Alliance to Restore the Republic" } } }).to_string(),
    )
    .unwrap();
    assert_eq!(affected, 1);

    let (total, rows) = rushdb_mongodb::execute_query(
        &lmpg,
        &json!({ "collection": "FACTION", "filter": { "name": "Alliance to Restore the Republic" } }).to_string(),
        &|| false,
    )
    .unwrap();
    assert_eq!(total, 0);

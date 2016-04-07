//! End-to-end coverage of the documented query document
//! (docs.rushdb.com/learn/search-query/): MongoDB-style `where` operators,
//! logical composition, `$id`, label-key relationship blocks, `hops`
//! variable-length traversal, `$cycle` detection, and query-document `select`/groupBy
//! lowering.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{import, Lmpg};
use rushdb_query::document::parse_document;
use rushdb_query::{
    aggregate, aggregate_to_rows, aggregate_with_cancel, find_records, find_records_with_cancel,
    QueryError,
};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TenantId::from_ordinal(71))
}

fn run(lmpg: &Lmpg, document: &Value) -> Vec<Value> {
    let q = parse_document(document).expect("valid query document");
    if let Some(agg) = q.aggregate {
        let buckets = aggregate(lmpg, &agg).expect("aggregate");
        buckets
            .iter()
            .map(|b| Value::Object(b.keys.clone()))
            .collect()
    } else {
        find_records(lmpg, &q.search).expect("search").records
    }
}

#[test]
fn in_nin_type_ends_with_and_logical_ops() {
    let lmpg = setup();
    for (name, status, premium, free, n) in [
        ("alice", "active", true, false, 5u64),
        ("bob", "pending", false, true, 3),
        ("carol", "archived", true, true, 7),
        ("dave", "deleted", false, false, 2),
    ] {
        lmpg.create_record(
            "USER",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!(name))),
                ("status".into(), FieldChange::Set(json!(status))),
                ("premium".into(), FieldChange::Set(json!(premium))),
                ("freeAccess".into(), FieldChange::Set(json!(free))),
                ("score".into(), FieldChange::Set(json!(n))),
            ],
        )
        .unwrap();
    }

    // $in
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"status": {"$in": ["active", "pending"]}}}),
    );
    assert_eq!(rows.len(), 2);

    // $nin
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"status": {"$nin": ["deleted", "archived"]}}}),
    );
    assert_eq!(rows.len(), 2);

    // $type string + $endsWith
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {
            "$and": [
                {"name": {"$type": "string"}},
                {"name": {"$endsWith": "e"}}
            ]
        }}),
    );
    assert_eq!(rows.len(), 2); // alice, dave

    // $gt + $lte combined on one field (implicit and in operator block)
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"score": {"$gte": 3, "$lte": 5}}}),
    );
    assert_eq!(rows.len(), 2); // alice(5), bob(3)

    // $xor: exactly one of premium / freeAccess
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"$xor": [{"premium": true}, {"freeAccess": true}]}}),
    );
    assert_eq!(rows.len(), 2); // alice, bob

    // $nor: not active and not pending
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"$nor": [{"status": "active"}, {"status": "pending"}]}}),
    );
    assert_eq!(rows.len(), 2); // carol, dave

    // $not + $and nested
    let rows = run(
        &lmpg,
        &json!({"labels": ["USER"], "where": {"$and": [
            {"premium": true},
            {"$not": {"status": "archived"}}
        ]}}),
    );
    assert_eq!(rows.len(), 1); // alice
}

#[test]
fn datetime_component_integer_overflow_is_rejected() {
    let result = parse_document(&json!({
        "where": { "createdAt": { "$year": 18_446_744_073_709_551_615u64 } }
    }));
    match result {
        Ok(_) => panic!("overflowing component should be rejected"),
        Err(err) => assert!(err.to_string().contains("expected integer")),
    }
}

#[test]
fn id_filter_and_flat_equality() {
    let lmpg = setup();
    let r = lmpg
        .create_record(
            "DOC",
            None,
            vec![("title".into(), FieldChange::Set(json!("hello")))],
        )
        .unwrap();

    let snap = lmpg.store.read_view().unwrap();
    let json = rushdb_lmpg::reconstruct::reconstruct_record(&snap, lmpg.tenant, r.ordinal)
        .unwrap()
        .unwrap();
    let id = json["__id"].as_str().unwrap().to_string();

    let rows = run(&lmpg, &json!({"labels": ["DOC"], "where": {"$id": id}}));
    assert_eq!(rows.len(), 1);

    let dashed = uuid::Uuid::from_bytes(r.record_id).to_string();
    let rows = run(
        &lmpg,
        &json!({"labels": ["DOC"], "where": {"$id": {"$in": [dashed]}}}),
    );
    assert_eq!(rows.len(), 1);

    // flat equality: { title: "hello" }
    let rows = run(
        &lmpg,
        &json!({"labels": ["DOC"], "where": {"title": "hello"}}),
    );
    assert_eq!(rows.len(), 1);
    // missing field doesn't match
    let rows = run(&lmpg, &json!({"labels": ["DOC"], "where": {"nope": true}}));
    assert_eq!(rows.len(), 0);
}

#[test]
fn inline_aggregate_operands_feed_derived_math() {
    let lmpg = setup();
    for (name, price) in [("a", 10), ("a", 20), ("b", 8)] {
        lmpg.create_record(
            "ITEM",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!(name))),
                ("price".into(), FieldChange::Set(json!(price))),
            ],
        )
        .unwrap();
    }
    let parsed = parse_document(&json!({
        "labels": ["ITEM"],
        "select": {
            "total": {"$count": "*"},
            "ratio": {"$divide": [
                {"$sum": "$record.price"},
                {"$avg": "$record.price", "$precision": 2}
            ]}
        },
        "groupBy": ["$record.name"]
    }))
    .unwrap();
    let aggregate_request = parsed.aggregate.unwrap();
    let buckets = aggregate(&lmpg, &aggregate_request).unwrap();
    let rows = aggregate_to_rows(&lmpg, &buckets, &aggregate_request).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter().all(|row| row.get("ratio").is_some()),
        "{rows:?}"
    );
    assert!(rows.iter().all(|row| {
        row.as_object()
            .is_some_and(|obj| obj.keys().all(|key| !key.starts_with("__ratio_")))
    }));
}

#[test]
fn related_label_block_and_alias() {
    let lmpg = setup();
    let tech = import::import_document(&lmpg, "COMPANY", &json!({"name": "Tech Corp"})).unwrap();
    let eng = import::import_document(
        &lmpg,
        "DEPARTMENT",
        &json!({"name": "Engineering", "headcount": 50}),
    )
    .unwrap();
    import::import_document(
        &lmpg,
        "DEPARTMENT",
        &json!({"name": "Sales", "headcount": 10}),
    )
    .unwrap();
    lmpg.create_relationship("HAS_DEPARTMENT", tech, eng, vec![])
        .unwrap();

    // basic related block with property filter
    let rows = run(
        &lmpg,
        &json!({"labels": ["COMPANY"], "where": {
            "DEPARTMENT": {"name": "Engineering", "headcount": {"$gte": 20}}
        }}),
    );
    assert_eq!(rows.len(), 1);

    // related block with $relation direction out + no match
    let rows = run(
        &lmpg,
        &json!({"labels": ["COMPANY"], "where": {
            "DEPARTMENT": {"$relation": {"type": "HAS_DEPARTMENT", "direction": "out"},
                           "name": "Sales"}
        }}),
    );
    assert_eq!(rows.len(), 0); // Sales is not a department of Tech
}

#[test]
fn variable_length_hops_traversal() {
    let lmpg = setup();
    // chain: a -> b -> c -> d
    let a = import::import_document(&lmpg, "EMP", &json!({"name": "A"})).unwrap();
    let b = import::import_document(&lmpg, "EMP", &json!({"name": "Alice"})).unwrap();
    let c = import::import_document(&lmpg, "EMP", &json!({"name": "C"})).unwrap();
    let d = import::import_document(&lmpg, "EMP", &json!({"name": "D"})).unwrap();
    lmpg.create_relationship("REPORTS_TO", a, b, vec![])
        .unwrap();
    lmpg.create_relationship("REPORTS_TO", b, c, vec![])
        .unwrap();
    lmpg.create_relationship("REPORTS_TO", c, d, vec![])
        .unwrap();

    // a's management chain up to 4 hops contains Alice (at depth 1)
    let rows = run(
        &lmpg,
        &json!({"labels": ["EMP"], "where": {
            "EMP": {"$relation": {"type": "REPORTS_TO", "direction": "out",
                                  "hops": {"min": 1, "max": 4}},
                    "name": {"$contains": "Alice"}}
        }}),
    );
    // every node whose chain reaches Alice: a (depth1), and none beyond
    assert_eq!(rows.len(), 1);

    // a's management chain reaches D at exactly 3 hops (a->b->c->d)
    let rows = run(
        &lmpg,
        &json!({"labels": ["EMP"], "where": {
            "EMP": {"$relation": {"type": "REPORTS_TO", "direction": "out",
                                  "hops": 3},
                    "name": "D"}
        }}),
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn cycle_detection_finds_rings() {
    let lmpg = setup();
    let a = import::import_document(&lmpg, "ACCOUNT", &json!({"n": "a"})).unwrap();
    let b = import::import_document(&lmpg, "ACCOUNT", &json!({"n": "b"})).unwrap();
    let c = import::import_document(&lmpg, "ACCOUNT", &json!({"n": "c"})).unwrap();
    let d = import::import_document(&lmpg, "ACCOUNT", &json!({"n": "d"})).unwrap();
    // ring a->b->c->a ; d isolated
    lmpg.create_relationship("TRANSFERRED_TO", a, b, vec![])
        .unwrap();
    lmpg.create_relationship("TRANSFERRED_TO", b, c, vec![])
        .unwrap();
    lmpg.create_relationship("TRANSFERRED_TO", c, a, vec![])
        .unwrap();
    lmpg.create_relationship("TRANSFERRED_TO", d, d, vec![])
        .unwrap();

    let rows = run(
        &lmpg,
        &json!({"labels": ["ACCOUNT"], "where": {
            "$cycle": {"type": "TRANSFERRED_TO", "direction": "out",
                       "hops": {"min": 2, "max": 6}}
        }}),
    );
    assert_eq!(rows.len(), 3); // a, b, c on the ring; d's self-loop is a 1-hop
}

#[test]
fn query_document_select_groupby_orderby() {
    let lmpg = setup();
    import::import_document(&lmpg, "ORDER", &json!({"status": "paid", "total": 100})).unwrap();
    import::import_document(&lmpg, "ORDER", &json!({"status": "paid", "total": 50})).unwrap();
    import::import_document(&lmpg, "ORDER", &json!({"status": "pending", "total": 25})).unwrap();

    // dimensional groupBy with $sum/$count, orderBy desc
    let q = parse_document(&json!({
        "labels": ["ORDER"],
        "select": {"count": {"$count": "*"}, "revenue": {"$sum": "$record.total"}},
        "groupBy": ["$record.status"],
        "orderBy": {"revenue": "desc"}
    }))
    .unwrap();
    let buckets = aggregate(&lmpg, q.aggregate.as_ref().unwrap()).unwrap();
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0].sums.get("revenue"), Some(&json!(150.0)));
    assert_eq!(buckets[0].count_roots, 2);

    // self-group KPI: groupBy on select key collapses to one global bucket
    let q = parse_document(&json!({
        "labels": ["ORDER"],
        "select": {"totalRevenue": {"$sum": "$record.total"}},
        "groupBy": ["totalRevenue"],
        "orderBy": {"totalRevenue": "asc"}
    }))
    .unwrap();
    let buckets = aggregate(&lmpg, q.aggregate.as_ref().unwrap()).unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].sums.get("totalRevenue"), Some(&json!(175.0)));
}

#[test]
fn plain_select_projects_each_matching_record_with_aliases() {
    let lmpg = setup();
    import::import_document(
        &lmpg,
        "STARSHIP",
        &json!({
            "name": "Millennium Falcon",
            "pilot_character_ids": ["character_han_solo", "character_chewbacca"]
        }),
    )
    .unwrap();
    import::import_document(
        &lmpg,
        "STARSHIP",
        &json!({
            "name": "X-wing",
            "pilot_character_ids": ["character_rey"]
        }),
    )
    .unwrap();

    let query = parse_document(&json!({
        "labels": ["STARSHIP"],
        "where": { "name": { "$contains": "falcon" } },
        "select": {
            "starship": "$record.name",
            "pilot_character_ids": "$record.pilot_character_ids"
        },
        "limit": 100
    }))
    .unwrap();
    let aggregate_request = query
        .aggregate
        .expect("select lowers to projection execution");
    let buckets = aggregate(&lmpg, &aggregate_request).unwrap();
    let rows = aggregate_to_rows(&lmpg, &buckets, &aggregate_request).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("starship"), Some(&json!("Millennium Falcon")));
    assert_eq!(
        rows[0].get("pilot_character_ids"),
        Some(&json!(["character_han_solo", "character_chewbacca"]))
    );
    assert!(rows.iter().all(|row| row
        .as_object()
        .is_some_and(|object| !object.contains_key("__rushdb_projection_root"))));
}

#[test]
fn count_star_order_and_aggregate_column_order_match_query_contract() {
    let lmpg = setup();
    for faction in [
        "faction_rebel_alliance",
        "faction_galactic_republic",
        "faction_rebel_alliance",
        "faction_sith",
        "faction_rebel_alliance",
        "faction_galactic_republic",
        "faction_rebel_alliance",
    ] {
        import::import_document(&lmpg, "BATTLE", &json!({ "winner_faction_id": faction })).unwrap();
    }

    let parsed = parse_document(&json!({
        "labels": ["BATTLE"],
        "select": {
            "faction": "$record.winner_faction_id",
            "battles_won": { "$count": "*" }
        },
        "groupBy": ["$record.winner_faction_id"],
        "orderBy": { "battles_won": "desc" }
    }))
    .unwrap();
    let request = parsed.aggregate.unwrap();
    let buckets = aggregate(&lmpg, &request).unwrap();
    let rows = aggregate_to_rows(&lmpg, &buckets, &request).unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| row["battles_won"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![4, 2, 1]
    );
    assert_eq!(
        rows[0].as_object().unwrap().keys().collect::<Vec<_>>(),
        vec!["winner_faction_id", "battles_won"]
    );
}

/// A group key keeps the property name as `groupBy` spells it, so a
/// camelCase property comes back under the name the caller used, and ordering
/// by that name still resolves.
#[test]
fn group_keys_keep_the_requested_property_spelling() {
    let lmpg = setup();
    for driver in ["driver_ayla", "driver_bo", "driver_ayla"] {
        import::import_document(&lmpg, "RACE", &json!({ "winningDriverRef": driver })).unwrap();
    }
    let parsed = parse_document(&json!({
        "labels": ["RACE"],
        "select": { "driver": "$record.winningDriverRef", "wins": { "$count": "*" } },
        "groupBy": ["$record.winningDriverRef"],
        "orderBy": { "winningDriverRef": "desc" }
    }))
    .unwrap();
    let request = parsed.aggregate.unwrap();
    let buckets = aggregate(&lmpg, &request).unwrap();
    let rows = aggregate_to_rows(&lmpg, &buckets, &request).unwrap();
    assert_eq!(
        rows,
        vec![
            json!({ "winningDriverRef": "driver_bo", "wins": 1 }),
            json!({ "winningDriverRef": "driver_ayla", "wins": 2 }),
        ]
    );
}

#[test]
fn id_ordering_uses_uuidv7_creation_order() {
    let lmpg = setup();
    for name in ["first", "second", "third"] {
        import::import_document(&lmpg, "ITEM", &json!({ "name": name })).unwrap();
    }

    let descending = run(&lmpg, &json!({ "labels": ["ITEM"], "orderBy": "desc" }));
    let descending_ids = descending
        .iter()
        .map(|record| record["__id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(descending_ids.windows(2).all(|pair| pair[0] > pair[1]));

    let ascending = run(
        &lmpg,
        &json!({ "labels": ["ITEM"], "orderBy": { "id": "asc" } }),
    );
    let ascending_ids = ascending
        .iter()
        .map(|record| record["__id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(ascending_ids.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn record_and_aggregate_scans_stop_cooperatively_when_cancelled() {
    let lmpg = setup();
    for value in 0..32 {
        import::import_document(&lmpg, "ITEM", &json!({ "value": value })).unwrap();
    }

    let parsed = parse_document(&json!({ "labels": ["ITEM"] })).unwrap();
    let probes = std::cell::Cell::new(0usize);
    let cancelled = || {
        probes.set(probes.get() + 1);
        probes.get() > 4
    };
    assert!(matches!(
        find_records_with_cancel(&lmpg, &parsed.search, &cancelled),
        Err(QueryError::Cancelled)
    ));

    let parsed = parse_document(&json!({
        "labels": ["ITEM"],
        "select": { "count": { "$count": "*" } },
        "groupBy": ["$record.value"]
    }))
    .unwrap();
    let probes = std::cell::Cell::new(0usize);
    let cancelled = || {
        probes.set(probes.get() + 1);
        probes.get() > 6
    };
    assert!(matches!(
        aggregate_with_cancel(&lmpg, parsed.aggregate.as_ref().unwrap(), &cancelled),
        Err(QueryError::Cancelled)
    ));
}

#[test]
fn datetime_iso_and_component_operators() {
    let lmpg = setup();
    // created stored as epoch-ms Int64 values:
    // 2023-01-01T00:00:00Z = 1672531200000
    // 2023-06-15T12:00:00Z = 1686823200000
    // 2024-01-01T00:00:00Z = 1704067200000
    for ms in [1_672_531_200_000i64, 1_686_823_200_000, 1_704_067_200_000] {
        lmpg.create_record(
            "EVENT",
            None,
            vec![("created".into(), FieldChange::Set(json!(ms)))],
        )
        .unwrap();
    }

    // $gte ISO string: on or after 2023-06-15
    let rows = run(
        &lmpg,
        &json!({"labels": ["EVENT"], "where": {"created": {"$gte": "2023-06-15T00:00:00Z"}}}),
    );
    assert_eq!(rows.len(), 2);

    // $lt component object (year 2024 boundary)
    let rows = run(
        &lmpg,
        &json!({"labels": ["EVENT"], "where": {"created": {"$lt": {"$year": 2024, "$month": 1, "$day": 1}}}}),
    );
    assert_eq!(rows.len(), 2);

    // $year component: records created in 2023
    let rows = run(
        &lmpg,
        &json!({"labels": ["EVENT"], "where": {"created": {"$year": 2023}}}),
    );
    assert_eq!(rows.len(), 2);

    // $year + $month: June 2023
    let rows = run(
        &lmpg,
        &json!({"labels": ["EVENT"], "where": {"$and": [
            {"created": {"$year": 2023}},
            {"created": {"$month": 6}}
        ]}}),
    );
    assert_eq!(rows.len(), 1);
}

/// ORDER BY must be a total order even when a field mixes types or is missing
/// on some records. A comparator returning `Equal` in a non-transitive way
/// (e.g. from a NaN `partial_cmp`) previously panicked `sort_by` with
/// "user-provided comparison function does not correctly implement a total
/// order".
#[test]
fn order_by_handles_nan_and_mixed_types_as_total_order() {
    let lmpg = setup();
    // a real number, a string, a missing value, and a large number on one field
    lmpg.create_record(
        "ITEM",
        None,
        vec![("birth_year_bby".into(), FieldChange::Set(json!(2000)))],
    )
    .unwrap();
    lmpg.create_record(
        "ITEM",
        None,
        vec![("birth_year_bby".into(), FieldChange::Set(json!("c")))],
    )
    .unwrap();
    lmpg.create_record("ITEM", None, vec![]).unwrap();
    lmpg.create_record(
        "ITEM",
        None,
        vec![("birth_year_bby".into(), FieldChange::Set(json!(100)))],
    )
    .unwrap();

    for order_by in [
        json!({"birth_year_bby": "asc"}),
        json!({"birth_year_bby": "desc"}),
    ] {
        let rows = run(
            &lmpg,
            &json!({"labels": ["ITEM"], "where": {}, "orderBy": order_by, "limit": 100}),
        );
        assert_eq!(rows.len(), 4, "orderBy {order_by} must not panic");
    }
}

/// Regression: `$count` over a related-record alias must count the distinct
/// related records, not be treated as a root-record field (all-zeros bug).
/// Query shape mirrors the Star Wars fixture: FACTION <-[:`HAS_WINNER_FACTION_ID`]-(BATTLE).
#[test]
fn count_over_related_alias_counts_distinct_related_records() {
    let lmpg = setup();
    // Two factions; a wins twice, b wins once, via BATTLE -> FACTION edges.
    let a = import::import_document(&lmpg, "FACTION", &json!({"name": "Jedi Order"})).unwrap();
    let b = import::import_document(&lmpg, "FACTION", &json!({"name": "Sith Order"})).unwrap();
    for _ in 0..2 {
        let battle = import::import_document(&lmpg, "BATTLE", &json!({"name": "b"})).unwrap();
        lmpg.create_relationship("HAS_WINNER_FACTION_ID", battle, a, vec![])
            .unwrap();
    }
    {
        let battle = import::import_document(&lmpg, "BATTLE", &json!({"name": "b"})).unwrap();
        lmpg.create_relationship("HAS_WINNER_FACTION_ID", battle, b, vec![])
            .unwrap();
    }

    let parsed = parse_document(&json!({
        "labels": ["FACTION"],
        "where": {
            "BATTLE": {
                "$alias": "$battle",
                "$relation": { "type": "HAS_WINNER_FACTION_ID", "direction": "in" }
            }
        },
        "select": {
            "faction": "$record.name",
            "wins": { "$count": "$battle" }
        },
        "groupBy": ["$record.name"],
        "orderBy": { "wins": "desc" }
    }))
    .unwrap();
    let req = parsed.aggregate.unwrap();
    let buckets = aggregate(&lmpg, &req).unwrap();
    let rows = aggregate_to_rows(&lmpg, &buckets, &req).unwrap();

    assert_eq!(
        rows.len(),
        2,
        "both factions have at least one winning battle"
    );
    assert_eq!(rows[0]["name"], json!("Jedi Order"));
    assert_eq!(rows[0]["wins"], json!(2), "Jedi Order won two battles");
    assert_eq!(rows[1]["name"], json!("Sith Order"));
    assert_eq!(rows[1]["wins"], json!(1), "Sith Order won one battle");
}

#[test]
fn collect_from_alias_gathers_related_records() {
    let lmpg = setup();
    let company = import::import_document(&lmpg, "Company", &json!({"name": "TechCorp"})).unwrap();
    let e1 = import::import_document(
        &lmpg,
        "Employee",
        &json!({"name": "Jane", "salary": 600_000}),
    )
    .unwrap();
    let e2 = import::import_document(
        &lmpg,
        "Employee",
        &json!({"name": "John", "salary": 550_000}),
    )
    .unwrap();
    lmpg.create_relationship("EMPLOYS", company, e1, vec![])
        .unwrap();
    lmpg.create_relationship("EMPLOYS", company, e2, vec![])
        .unwrap();

    let rows = run(
        &lmpg,
        &json!({
            "labels": ["Company"],
            "where": { "Employee": { "$alias": "$employee" } },
            "select": {
                "employeeNames": {
                    "$collect": {
                        "from": "$employee",
                        "select": { "name": "$employee.name" },
                        "orderBy": { "name": "asc" },
                        "limit": 10
                    }
                }
            }
        }),
    );
    let names: Vec<String> = rows[0]["employeeNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["Jane".to_string(), "John".to_string()]);
}

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

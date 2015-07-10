//! Parity verification against the actual rushdb-repo `SearchQuery` test
//! syntax (platform/core/src/core/search/parser/tests/*.spec.ts). These
//! cases are ported verbatim from the TypeScript suite to confirm the Rust
//! query-document parser accepts the same documented syntax.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{import, Lmpg};
use rushdb_query::document::parse_document;
use rushdb_query::{aggregate, find_records, find_records_in_view, find_records_streaming};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TenantId::from_ordinal(81))
}

fn match_count(lmpg: &Lmpg, document: &Value) -> usize {
    let q = parse_document(document).expect("valid query document");
    if let Some(agg) = q.aggregate {
        aggregate(lmpg, &agg).expect("aggregate").len()
    } else {
        usize::try_from(find_records(lmpg, &q.search).expect("search").total_matched)
            .unwrap_or(usize::MAX)
    }
}

/// parseQuery.spec.ts "parses subQueries correctly 1":
/// { tag: 'top-sellers', AUTHOR: { name: 'James Brown', $relation: {...} } }
#[test]
fn ported_parse_query_1() {
    let lmpg = setup();
    let book = import::import_document(&lmpg, "BOOK", &json!({"title": "T"})).unwrap();
    let tag = import::import_document(&lmpg, "BOOK", &json!({"tag": "top-sellers"})).unwrap();
    let author = import::import_document(&lmpg, "AUTHOR", &json!({"name": "James Brown"})).unwrap();
    import::import_document(&lmpg, "AUTHOR", &json!({"name": "Someone Else"})).unwrap();
    // top-seller tagged book authored by James Brown
    lmpg.create_relationship("AUTHORED", author, tag, vec![])
        .unwrap();
    let _ = book;

    let n = match_count(
        &lmpg,
        &json!({"labels": ["BOOK"], "where": {
            "tag": "top-sellers",
            "AUTHOR": {"name": "James Brown", "$relation": {"type": "AUTHORED", "direction": "in"}}
        }}),
    );
    assert_eq!(n, 1);
}

/// parseQuery.spec.ts "parses subQueries correctly 2": logical ops inside a
/// field block, datetime components, nested label blocks.
#[test]
fn ported_parse_query_2_field_logical_and_datetime() {
    let lmpg = setup();
    // rating in [4.5..6] or ==3 or not >=4; created 2011-11-11
    lmpg.create_record(
        "POST",
        None,
        vec![
            (
                "created".into(),
                FieldChange::Set(json!(1_320_969_600_000i64)),
            ), // 2011-11-11
            ("rating".into(), FieldChange::Set(json!(5))),
        ],
    )
    .unwrap();
    // rating 3, created 2012
    lmpg.create_record(
        "POST",
        None,
        vec![
            (
                "created".into(),
                FieldChange::Set(json!(1_336_176_000_000i64)),
            ), // 2012-05-05
            ("rating".into(), FieldChange::Set(json!(3))),
        ],
    )
    .unwrap();
    // rating 2 (not >=4), created 2011
    lmpg.create_record(
        "POST",
        None,
        vec![
            (
                "created".into(),
                FieldChange::Set(json!(1_320_969_600_000i64)),
            ),
            ("rating".into(), FieldChange::Set(json!(2))),
        ],
    )
    .unwrap();

    let n = match_count(
        &lmpg,
        &json!({"labels": ["POST"], "where": {
            "created": {"$year": 2011, "$month": 11, "$day": 11},
            "rating": {"$or": [
                {"$and": [{"$gt": 4.5}, {"$lt": 6}]},
                {"$ne": 3},
                {"$not": {"$gte": 4}}
            ]}
        }}),
    );
    // created exactly 2011-11-11 AND rating satisfies the OR group:
    // record0: 5 ∈ (4.5,6) ✓ ; record2: 2 (not>=4) ✓
    assert_eq!(n, 2);
}

/// compileExpr.spec.ts select scenarios: $count '*', $sum of nested math,
/// $avg with precision, $ref derived output, $record.__id.
#[test]
fn ported_compile_select_scenarios() {
    let lmpg = setup();
    import::import_document(
        &lmpg,
        "ORDER",
        &json!({"price": 10, "qty": 2, "amount": 20}),
    )
    .unwrap();
    import::import_document(
        &lmpg,
        "ORDER",
        &json!({"price": 15, "qty": 1, "amount": 15}),
    )
    .unwrap();

    // $count '*' + $sum over $record.amount
    let q = parse_document(&json!({
        "labels": ["ORDER"],
        "select": {"count": {"$count": "*"}, "revenue": {"$sum": "$record.amount"}}
    }))
    .unwrap();
    let buckets = aggregate(&lmpg, q.aggregate.as_ref().unwrap()).unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].count_roots, 2);
    assert_eq!(buckets[0].sums.get("revenue"), Some(&json!(35.0)));

    // $sum over nested math ($multiply price*qty)
    let q = parse_document(&json!({
        "labels": ["ORDER"],
        "select": {"lineTotal": {"$sum": {"$multiply": ["$record.price", "$record.qty"]}}}
    }))
    .unwrap();
    let buckets = aggregate(&lmpg, q.aggregate.as_ref().unwrap()).unwrap();
    assert_eq!(buckets[0].sums.get("lineTotal"), Some(&json!(35.0))); // 10*2 + 15*1

    // $avg with $precision
    let q = parse_document(&json!({
        "labels": ["ORDER"],
        "select": {"avgAmount": {"$avg": "$record.amount", "$precision": 1}}
    }))
    .unwrap();
    let buckets = aggregate(&lmpg, q.aggregate.as_ref().unwrap()).unwrap();
    assert_eq!(buckets[0].sums.get("avgAmount"), Some(&json!(17.5))); // (20+15)/2

    // $subtract with $ref operands: revenue - cost
    let q = parse_document(&json!({
        "labels": ["ORDER"],
        "select": {
            "revenue": {"$sum": "$record.amount"},
            "cost": {"$sum": "$record.qty"},
            "margin": {"$subtract": [{"$ref": "revenue"}, {"$ref": "cost"}]}
        }
    }))
    .unwrap();
    let buckets = aggregate(&lmpg, q.aggregate.as_ref().unwrap()).unwrap();
    assert_eq!(buckets[0].sums.get("margin"), Some(&json!(32.0))); // 35 - 3
}

/// Array-element semantics: { tags: 'typescript' } matches arrays containing it.
#[test]
fn array_element_semantics() {
    let lmpg = setup();
    lmpg.create_record(
        "ITEM",
        None,
        vec![(
            "tags".into(),
            FieldChange::Set(json!(["javascript", "typescript", "react"])),
        )],
    )
    .unwrap();
    lmpg.create_record(
        "ITEM",
        None,
        vec![("tags".into(), FieldChange::Set(json!(["python", "django"])))],
    )
    .unwrap();

    let n = match_count(
        &lmpg,
        &json!({"labels": ["ITEM"], "where": {"tags": "typescript"}}),
    );
    assert_eq!(n, 1);

    // $in over arrays: any element in list
    let n = match_count(
        &lmpg,
        &json!({"labels": ["ITEM"], "where": {"tags": {"$in": ["python"]}}}),
    );
    assert_eq!(n, 1);

    // $contains on array element
    let n = match_count(
        &lmpg,
        &json!({"labels": ["ITEM"], "where": {"tags": {"$contains": "react"}}}),
    );
    assert_eq!(n, 1);
}

/// Array-literal equality `{ tags: ["a", "b"] }` lowers to element-match
/// ($in) semantics instead of a label-key relationship block, matching the
/// baseline behavior of wrapping single values into IN predicates.
#[test]
fn array_literal_value_is_element_match() {
    let lmpg = setup();
    lmpg.create_record(
        "ITEM",
        None,
        vec![(
            "tags".into(),
            FieldChange::Set(json!(["javascript", "typescript"])),
        )],
    )
    .unwrap();
    lmpg.create_record(
        "ITEM",
        None,
        vec![("tags".into(), FieldChange::Set(json!(["python"])))],
    )
    .unwrap();
    lmpg.create_record(
        "ITEM",
        None,
        vec![("tags".into(), FieldChange::Set(json!("single")))],
    )
    .unwrap();

    // { tags: ["typescript"] } matches the record containing that element
    let n = match_count(
        &lmpg,
        &json!({"labels": ["ITEM"], "where": {"tags": ["typescript"]}}),
    );
    assert_eq!(n, 1);

    // { tags: ["python", "ruby"] } matches any element in the list
    let n = match_count(
        &lmpg,
        &json!({"labels": ["ITEM"], "where": {"tags": ["python", "ruby"]}}),
    );
    assert_eq!(n, 1);

    // scalar field values are matched by the same element semantics
    let n = match_count(
        &lmpg,
        &json!({"labels": ["ITEM"], "where": {"tags": ["single", "missing"]}}),
    );
    assert_eq!(n, 1);

    // $nin over an array literal list: records with no element in the list
    let n = match_count(
        &lmpg,
        &json!({"labels": ["ITEM"], "where": {"tags": {"$nin": ["javascript", "python", "single"]}}}),
    );
    assert_eq!(n, 0);
}

/// `$type` is cardinality-aware: `string[]`/`number[]` match array-typed
/// columns, `array` matches any array, and the bare type matches both scalar
/// and array values (baseline stores array primitives as plain string).
#[test]
fn type_operator_is_array_cardinality_aware() {
    let lmpg = setup();
    lmpg.create_record(

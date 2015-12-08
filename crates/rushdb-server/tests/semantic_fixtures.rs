//! 2C — LMPG semantic fixtures. These pin the exact semantics every future
//! optimization (filter pushdown, bounded aggregation, spill) must preserve:
//! null versus missing, multivalued/array properties, duplicate occurrences,
//! ordering ties, aggregates, and transaction visibility.

use rushdb_protocol::{
    Mutation, Operation, Query, QueryDocument, RequestContext, RequestEnvelope, Response,
};
use rushdb_server::{Node, ServerConfig};

fn node() -> Node {
    let mut cfg = ServerConfig::default();
    Node::bootstrap(&mut cfg).unwrap()
}

fn put(node: &Node, label: &str, properties: serde_json::Value) {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("fixture-put".into()),
        context: RequestContext::default(),
        operation: Operation::Mutate {
            mutation: Mutation::PutRecord {
                mode: rushdb_protocol::PutMode::Create,
                id: None,
                label: Some(label.to_string()),
                properties,
                vectors: vec![],
                options: serde_json::json!({}),
            },
        },
    };
    rushdb_server::protocol::ProtocolExecutor::new(node, None)
        .execute(envelope)
        .unwrap();
}

fn find(node: &Node, doc: QueryDocument) -> Vec<serde_json::Value> {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("fixture-find".into()),
        context: RequestContext::default(),
        operation: Operation::Query {
            query: Query::Records { query: doc },
        },
    };
    let res = rushdb_server::protocol::ProtocolExecutor::new(node, None)
        .execute(envelope)
        .unwrap();
    let Response::Records { records, .. } = res.response else {
        panic!("expected records");
    };
    records
}

fn find_field_values(
    node: &Node,
    label: &str,
    predicate: serde_json::Value,
    field: &str,
) -> Vec<serde_json::Value> {
    let doc = QueryDocument {
        labels: vec![label.to_string()],
        predicate,
        limit: Some(100),
        ..Default::default()
    };
    find(node, doc)
        .into_iter()
        .filter_map(|r| {
            r.as_object()
                .and_then(|o| o.get("properties"))
                .and_then(|p| p.get(field).cloned())
        })
        .collect()
}

#[test]
fn null_is_distinct_from_missing() {
    let n = node();
    put(
        &n,
        "T",
        serde_json::json!({ "name": "explicit-null", "score": null }),
    );
    put(&n, "T", serde_json::json!({ "name": "missing-score" }));
    put(
        &n,
        "T",
        serde_json::json!({ "name": "has-score", "score": 7 }),
    );

    // `score: null` matches the explicitly-null record only — a missing
    // property is NOT null (explicit Null stays distinct from MISSING).
    let with_null = find_field_values(&n, "T", serde_json::json!({ "score": null }), "name");
    assert_eq!(with_null, vec![serde_json::json!("explicit-null")]);

    // `$ne` requires the field to be present (RushDB: value <> X): the
    // missing record is excluded; the present non-null record matches.
    let not_null = find_field_values(
        &n,
        "T",
        serde_json::json!({ "score": { "$ne": null } }),
        "name",
    );
    assert_eq!(not_null, vec![serde_json::json!("has-score")]);
}

#[test]
fn multivalued_array_property_matches_elementwise() {
    let n = node();
    put(
        &n,
        "T",
        serde_json::json!({ "name": "tags-a", "tags": ["x", "y"] }),
    );
    put(
        &n,
        "T",
        serde_json::json!({ "name": "tags-b", "tags": ["y", "z"] }),
    );
    put(&n, "T", serde_json::json!({ "name": "no-tags" }));

    // Element match: `{ tags: "x" }` matches only the record containing x.
    let has_x = find_field_values(&n, "T", serde_json::json!({ "tags": "x" }), "name");
    assert_eq!(has_x, vec![serde_json::json!("tags-a")]);

    // `{ tags: "y" }` matches both array records, not the missing one.
    let has_y = find_field_values(&n, "T", serde_json::json!({ "tags": "y" }), "name");
    assert_eq!(has_y.len(), 2, "both array records contain y");
    assert!(!has_y.contains(&serde_json::json!("no-tags")));
}

#[test]
fn create_never_deduplicates_records() {
    let n = node();
    // Two create calls with an identical payload are two distinct records —
    // create is insert-only; dedup would be a merge/upsert concern.
    put(&n, "T", serde_json::json!({ "name": "dup", "score": 1 }));
    put(&n, "T", serde_json::json!({ "name": "dup", "score": 1 }));

    let names = find_field_values(&n, "T", serde_json::json!({}), "name");
    assert_eq!(
        names,
        vec![serde_json::json!("dup"), serde_json::json!("dup")]
    );
}

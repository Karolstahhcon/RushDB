//! Facets counted from occurrence rows must agree with the records response
//! for the same query document: labels, per-label property support and array
//! shape, and property values with their counts.

use super::*;
use rushdb_protocol::{Describe, PropertyValuesMode, Query, RequestContext, RequestEnvelope};
use serde_json::json;
use std::collections::BTreeMap;

fn run(node: &Node, operation: Operation) -> Response {
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation,
        })
        .unwrap()
        .response
}

fn document(labels: &[&str], predicate: &serde_json::Value) -> QueryDocument {
    QueryDocument {
        labels: labels.iter().map(ToString::to_string).collect(),
        predicate: predicate.clone(),
        ..QueryDocument::default()
    }
}

fn seeded_node() -> Node {
    let mut cfg = super::tests::config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let rows = [
        (
            "Person",
            json!({"name": "ann", "age": 31, "score": 1, "tags": ["x", "y"], "city": "Riga"}),
        ),
        (
            "Person",
            json!({"name": "bob", "age": 25, "score": 1.5, "tags": [], "city": "Oslo"}),
        ),
        (
            "Person",
            json!({"name": "cat", "age": 40, "score": 1, "nick": null, "city": "Riga"}),
        ),
        (
            "Person",
            json!({"name": "dan", "age": 31, "tags": ["y"], "born": "2020-01-02T03:04:05Z"}),
        ),
        (
            "Company",
            json!({"name": "acme", "age": 99, "score": 7, "city": "Riga"}),
        ),
        (
            "Company",
            json!({"name": "init", "city": "Oslo", "tags": ["x"]}),
        ),
        (
            "Company",
            json!({"name": "void", "score": null, "city": "Riga"}),
        ),
    ];
    for (label, properties) in rows {
        node.scope()
            .create_record(label, &properties, &[], &json!({}))
            .unwrap();
    }
    node
}

/// `(label, properties)` of every record the records response returns.
fn oracle(
    node: &Node,
    query: &QueryDocument,
) -> Vec<(String, serde_json::Map<String, serde_json::Value>)> {
    let Response::Records { records, .. } = run(
        node,
        Operation::Query {
            query: Query::Records {
                query: query.clone(),
            },
        },
    ) else {
        panic!("expected records")
    };
    records
        .into_iter()
        .map(|record| {
            (
                record["label"].as_str().unwrap().to_string(),
                record["properties"].as_object().unwrap().clone(),
            )
        })
        .collect()
}

type Matched = [(String, serde_json::Map<String, serde_json::Value>)];

/// Labels and schema label counts against the matched records.
fn check_labels(node: &Node, query: &QueryDocument, expected: &Matched) {
    let context = format!("{:?} {}", query.labels, query.predicate);
    let Response::Labels { labels: mut got } = run(
        node,
        Operation::Describe {
            describe: Describe::Labels {
                query: query.clone(),
            },
        },
    ) else {
        panic!("expected labels")
    };
    got.sort();
    let mut want: Vec<String> = expected.iter().map(|(label, _)| label.clone()).collect();
    want.sort();
    want.dedup();
    assert_eq!(got, want, "labels for {context}");
    let Response::Schema { schema } = run(node, schema_request(query)) else {
        panic!("expected schema")
    };
    let mut want_counts: BTreeMap<String, u64> = BTreeMap::new();
    for (label, _) in expected {
        *want_counts.entry(label.clone()).or_default() += 1;
    }
    let got_counts: BTreeMap<String, u64> = schema
        .labels
        .iter()
        .map(|label| (label.name.clone(), label.records_count))
        .collect();
    assert_eq!(got_counts, want_counts, "schema labels for {context}");
}

fn schema_request(query: &QueryDocument) -> Operation {
    Operation::Describe {
        describe: Describe::Schema {
            query: query.clone(),
            detail: None,
            skip: None,
            limit: None,
        },
    }
}

/// Support per (label, name), summed over typed siblings, and array shape.
fn check_properties(node: &Node, query: &QueryDocument, expected: &Matched) {
    let mut want: BTreeMap<(String, String), (u64, bool)> = BTreeMap::new();
    for (label, properties) in expected {
        for (name, value) in properties {
            // A null is support, on a typed property and on one that is only
            // ever null (described with type `null`).
            let slot = want.entry((label.clone(), name.clone())).or_default();
            slot.0 += 1;
            slot.1 |= value.is_array();
        }
    }
    let Response::Schema { schema } = run(node, schema_request(query)) else {
        panic!("expected schema")
    };
    let mut got: BTreeMap<(String, String), (u64, bool)> = BTreeMap::new();
    for entry in &schema.properties {
        let slot = got
            .entry((entry.label.clone().unwrap(), entry.name.clone()))
            .or_default();
        slot.0 += entry.records_count;
        slot.1 |= entry.is_array;
    }
    assert_eq!(
        got, want,
        "properties for {:?} {}",
        query.labels, query.predicate
    );
}

/// First-seen distinct values, distinct count and top-k counts.
fn check_values(node: &Node, query: &QueryDocument, expected: &Matched, property: &str) {
    let context = format!("{property} for {:?} {}", query.labels, query.predicate);
    let mut want: Vec<(serde_json::Value, u64)> = Vec::new();
    for (_, properties) in expected {
        let Some(value) = properties.get(property) else {
            continue;
        };
        match want.iter_mut().find(|(seen, _)| seen == value) {
            Some(slot) => slot.1 += 1,
            None => want.push((value.clone(), 1)),
        }
    }
    let ask = |mode| {
        let Response::PropertyValues {
            values,
            distinct_count,
            counts,
            ..
        } = run(
            node,
            Operation::Query {
                query: Query::PropertyValues {
                    property: property.into(),
                    query: query.clone(),
                    mode,
                    limit: None,
                    search: None,
                },
            },
        )
        else {
            panic!("expected property values")
        };
        (values, distinct_count, counts)
    };
    let (values, distinct, _) = ask(None);
    let scoped = !(query.labels.is_empty() && query.predicate.as_object().unwrap().is_empty());
    if scoped {
        // By descending record count, ties by the value's JSON text.
        let mut ranked = want.clone();
        ranked.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.to_string().cmp(&b.0.to_string()))
        });
        let order: Vec<_> = ranked.into_iter().map(|(value, _)| value).collect();
        assert_eq!(values, order, "values {context}");
    }
    assert_eq!(distinct, Some(want.len() as u64), "distinct {context}");
    // Top-k counts: from the dictionaries when unfiltered (also under a
    // label), else from the match set.
    let (top, _, counts) = ask(Some(PropertyValuesMode::TopK));
    assert_eq!(top.len(), counts.len());
    for (value, count) in top.iter().zip(&counts) {
        let expected_count = want
            .iter()
            .find(|(seen, _)| seen == value)
            .unwrap_or_else(|| panic!("top-k value {value} not in records: {context}"))
            .1;
        assert_eq!(*count, expected_count, "top-k count of {value}: {context}");
    }
}

#[test]
fn facets_agree_with_the_records_response() {
    let node = seeded_node();
    let filters = [
        (vec![], json!({})),
        (vec![], json!({"city": "Riga"})),
        (vec![], json!({"age": {"$gte": 31}})),
        (vec!["Person"], json!({})),
        (vec!["Person"], json!({"city": {"$in": ["Oslo", "Riga"]}})),
    ];
    for (labels, predicate) in &filters {
        let query = document(labels, predicate);
        let expected = oracle(&node, &query);
        check_labels(&node, &query, &expected);
        check_properties(&node, &query, &expected);
        for property in ["name", "age", "score", "tags", "nick", "born", "city"] {
            check_values(&node, &query, &expected, property);
        }
    }
}

#[test]
fn value_facets_honour_the_property_mask() {
    let mut cfg = super::tests::config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for (label, properties) in [
        ("Allowed", json!({"v": 1, "secret": "x"})),
        ("Denied", json!({"v": 2, "secret": "y"})),
    ] {
        node.scope()
            .create_record(label, &properties, &[], &json!({}))
            .unwrap();
    }
    node.scope()
        .security_put_policy(
            &rushdb_authz::DataAccessPolicy {
                allowed_labels: vec!["Allowed".into()],
                visible_properties: BTreeMap::from([("Allowed".into(), vec!["v".into()])]),
                ..Default::default()
            },
            rushdb_authz::AuthorizationMode::PolicyEnforced,
        )
        .unwrap();
    // As in the records response: the denied label is absent and the masked
    // property is hidden.
    for (property, want) in [("v", vec![json!(1)]), ("secret", vec![])] {
        let Response::PropertyValues { values, .. } = run(
            &node,
            Operation::Query {
                query: Query::PropertyValues {
                    property: property.into(),
                    query: QueryDocument::default(),
                    mode: None,
                    limit: None,
                    search: None,
                },
            },
        ) else {
            panic!("expected property values")
        };
        assert_eq!(values, want, "{property}");
    }
}

/// Ids of the records the records response returns for `query`.
fn matched_ids(node: &Node, query: &QueryDocument) -> std::collections::BTreeSet<String> {
    let Response::Records { records, .. } = run(
        node,
        Operation::Query {
            query: Query::Records {
                query: query.clone(),
            },
        },
    ) else {
        panic!("expected records")
    };
    records
        .iter()
        .map(|record| record["id"].as_str().unwrap().to_string())
        .collect()
}

type Shapes = BTreeMap<(String, String, String), (u64, BTreeMap<String, u64>)>;

/// Relationship shapes touching `ids` (either endpoint), from the
/// relationships response: count and per-property non-null counts.
fn shapes_touching(node: &Node, ids: &std::collections::BTreeSet<String>) -> Shapes {
    let Response::Relationships { relationships, .. } = run(
        node,
        Operation::Query {
            query: Query::Relationships {
                query: rushdb_protocol::RelationshipQuery {
                    limit: Some(10_000),
                    ..Default::default()
                },
            },
        },
    ) else {
        panic!("expected relationships")
    };
    let mut shapes = Shapes::new();
    for row in relationships {
        let touches = [&row["sourceId"], &row["targetId"]]
            .iter()
            .any(|end| end.as_str().is_some_and(|id| ids.contains(id)));
        if !touches {
            continue;
        }
        let key = (
            row["sourceLabel"].as_str().unwrap().to_string(),
            row["type"].as_str().unwrap().to_string(),
            row["targetLabel"].as_str().unwrap().to_string(),
        );
        let shape = shapes.entry(key).or_default();
        shape.0 += 1;
        for (name, value) in row["properties"].as_object().unwrap() {
            if !value.is_null() {
                *shape.1.entry(name.clone()).or_default() += 1;
            }
        }
    }
    shapes
}

fn link(node: &Node, rel_type: &str, from: &str, to: &str, properties: &serde_json::Value) {
    let command: rushdb_protocol::RelationshipMutation = serde_json::from_value(json!({
        "relationType": rel_type,
        "source": {"label": "Person", "predicate": {"name": from}},
        "target": {"label": "Person", "predicate": {"name": to}},
        "allowCartesian": true,
        "properties": properties,
    }))
    .unwrap();
    node.scope().link_records(&command).unwrap();
}

#[test]
fn relationship_facets_narrow_with_the_match_set() {
    let mut cfg = super::tests::config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for (name, city, employer) in [
        ("ann", "Riga", "acme"),
        ("bob", "Oslo", "init"),
        ("cat", "Riga", "acme"),
    ] {
        node.scope()
            .import_json_document(
                "Person",
                &json!({"name": name, "city": city, "employer": {"name": employer}}),
            )
            .unwrap();
    }
    link(&node, "KNOWS", "ann", "bob", &json!({"since": 2020}));
    link(&node, "KNOWS", "bob", "ann", &json!({}));
    link(&node, "SELF", "cat", "cat", &json!({"note": "x"}));
    let filters = [
        (vec![], json!({})),
        (vec!["Person"], json!({})),
        (vec![], json!({"city": "Riga"})),
        (vec!["Person"], json!({"name": "bob"})),
        (vec!["Person"], json!({"city": "Oslo"})),
        (vec![], json!({"name": "acme"})),
    ];
    for (labels, predicate) in &filters {
        let query = document(labels, predicate);
        let want = shapes_touching(&node, &matched_ids(&node, &query));
        let Response::Schema { schema } = run(&node, schema_request(&query)) else {
            panic!("expected schema")
        };
        let got: Shapes = schema
            .relationships
            .iter()
            .map(|shape| {
                (
                    (
                        shape.source_label.clone(),
                        shape.relationship_type.clone(),
                        shape.target_label.clone(),
                    ),
                    (
                        shape.count,
                        shape
                            .properties
                            .iter()
                            .map(|property| (property.name.clone(), property.relationships_count))
                            .collect(),
                    ),
                )
            })
            .collect();
        let filtered = !predicate.as_object().unwrap().is_empty();
        if filtered {
            assert_eq!(got, want, "relationships for {labels:?} {predicate}");
        } else {
            // The catalog reports property counts per relationship type, so
            // only the shapes and their counts are compared here.
            let counts = |shapes: &Shapes| -> BTreeMap<(String, String, String), u64> {
                shapes
                    .iter()
                    .map(|(key, (count, _))| (key.clone(), *count))
                    .collect()
            };
            assert_eq!(
                counts(&got),
                counts(&want),
                "relationships for {labels:?} {predicate}"
            );
        }
    }
}

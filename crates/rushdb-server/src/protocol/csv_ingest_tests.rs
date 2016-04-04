//! `ingest.csv` decodes rows and ingests them as documents (typed values,
//! per-row inline vectors, decoding errors); `query.exportCsv` renders the
//! records a query selects.

use super::*;
use rushdb_protocol::{
    CsvParseConfig, RequestContext, RequestEnvelope, VectorIndexDefinition, VectorQuery,
};

fn try_execute(node: &Node, operation: Operation) -> Result<Response, ProtocolError> {
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: None,
                deadline_ms: None,
                ..Default::default()
            },
            operation,
        })
        .map(|envelope| envelope.response)
}

fn execute(node: &Node, operation: Operation) -> Response {
    try_execute(node, operation).unwrap()
}

fn ingest_csv(
    label: &str,
    data: &str,
    options: serde_json::Value,
    vectors: Vec<serde_json::Value>,
) -> Operation {
    Operation::Ingest {
        ingest: Ingest::Csv {
            label: label.into(),
            data: data.into(),
            parse_config: CsvParseConfig::default(),
            options,
            vectors,
        },
    }
}

fn books() -> QueryDocument {
    QueryDocument {
        labels: vec!["Book".into()],
        ..QueryDocument::default()
    }
}

fn records(node: &Node, query: QueryDocument) -> Vec<serde_json::Value> {
    match execute(
        node,
        Operation::Query {
            query: Query::Records { query },
        },
    ) {
        Response::Records { records, .. } => records,
        other => panic!("expected records, got {other:?}"),
    }
}

#[test]
fn csv_rows_become_typed_records_and_export_renders_them() {
    let node = Node::bootstrap(&mut super::tests::config(false)).unwrap();
    let ingested = execute(
        &node,
        ingest_csv(
            "Book",
            "title,pages,in print\nDune,412,true\n\"Smith, J\",,false\n",
            serde_json::json!({ "suggestTypes": true }),
            Vec::new(),
        ),
    );
    assert!(
        matches!(ingested, Response::Ingested { count: 2, .. }),
        "{ingested:?}"
    );

    let stored = records(&node, books());
    let dune = stored
        .iter()
        .find(|record| record["properties"]["title"] == "Dune")
        .expect("Dune");
    assert_eq!(dune["properties"]["pages"], 412);
    assert_eq!(dune["properties"]["in_print"], true);
    let smith = stored
        .iter()
        .find(|record| record["properties"]["title"] == "Smith, J")
        .expect("Smith, J");
    assert!(
        smith["properties"].get("pages").is_none(),
        "an empty typed cell is left out"
    );

    let Response::Csv { content, count } = execute(
        &node,
        Operation::Query {
            query: Query::ExportCsv { query: books() },
        },
    ) else {
        panic!("expected csv");
    };
    assert_eq!(count, 2);
    assert_eq!(content, csv::render(&stored));
    assert!(
        content.starts_with("__id,__label,in_print,pages,title\r\n"),
        "{content}"
    );
    assert!(content.contains(",Book,true,412,Dune"), "{content}");
    assert!(content.contains(",Book,false,,\"Smith, J\""), "{content}");
}

#[test]
fn csv_rows_carry_their_inline_vectors() {
    let node = Node::bootstrap(&mut super::tests::config(false)).unwrap();
    execute(
        &node,
        Operation::Index {
            index: IndexOperation::CreateVector {
                definition: VectorIndexDefinition {
                    label: "Book".into(),
                    property: "title".into(),
                    dimensions: 2,
                    similarity: "cosine".into(),
                    name: Some("book-titles".into()),
                    model: None,
                    owner: None,
                },
            },
        },
    );
    let vector = |x: f32, y: f32| serde_json::json!([{ "propertyName": "title", "vector": [x, y], "index": "book-titles" }]);
    execute(
        &node,
        ingest_csv(
            "Book",
            "title\nDune\nEmma\n",
            serde_json::json!({}),
            vec![vector(1.0, 0.0), vector(0.0, 1.0)],
        ),
    );
    let Response::Records { records, .. } = execute(
        &node,
        Operation::Query {
            query: Query::Vector {
                query: VectorQuery {
                    labels: vec!["Book".into()],
                    property: "title".into(),
                    vector: vec![0.0, 1.0],
                    similarity: "cosine".into(),
                    index: Some("book-titles".into()),
                    predicate: serde_json::json!({}),
                    offset: 0,
                    limit: 1,
                    ef: None,
                    mode: None,
                    score_threshold: None,
                },
            },
        },
    ) else {
        panic!("expected records");
    };
    assert_eq!(records[0]["properties"]["title"], "Emma");
}

#[test]

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

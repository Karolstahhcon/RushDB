//! Text indexes are searchable from every statement language, through the
//! node's one set of text indexes: SQL's `text_search` table function.

use super::*;
use rushdb_protocol::{RequestContext, RequestEnvelope, StatementLanguage};

fn run(node: &Node, operation: Operation) -> Result<Response, ProtocolError> {
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation,
        })
        .map(|envelope| envelope.response)
}

fn mutate(node: &Node, language: StatementLanguage, statement: &str) {
    run(
        node,
        Operation::Mutate {
            mutation: Mutation::Statement {
                language,
                statement: statement.into(),
                parameters: None,
            },
        },
    )
    .unwrap();
}

/// Rows of a read statement.
fn rows(node: &Node, language: StatementLanguage, statement: &str) -> Vec<serde_json::Value> {
    match run(
        node,
        Operation::Query {
            query: Query::Statement {
                language,
                statement: statement.into(),
                parameters: None,
            },
        },
    ) {
        Ok(Response::Rows { rows, .. }) => rows,
        other => panic!("{language:?} `{statement}`: {other:?}"),
    }
}

/// Articles with `immediate` text indexes on `title` and on every string.
fn articles() -> Node {
    let mut cfg = super::tests::config(false);
    cfg.languages.sql = true;
    cfg.languages.mongodb = true;
    cfg.languages.cypher = true;
    cfg.languages.gql = true;
    let node = Node::bootstrap(&mut cfg).unwrap();
    for (title, body) in [
        ("Graph databases", "nodes and edges in a graph"),
        ("Cooking pasta", "boil water, add salt"),
        ("Database internals", "storage engines and a graph of pages"),
    ] {
        node.scope()
            .create_record(
                "Article",
                &serde_json::json!({ "title": title, "body": body }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    mutate(
        &node,
        StatementLanguage::Sql,
        "CREATE INDEX titles ON Article(title) USING TEXT IMMEDIATE",
    );
    mutate(
        &node,
        StatementLanguage::Mongo,
        r#"{"createIndex": {"collection": "Article", "property": "*", "kind": "text", "name": "everything", "consistency": "immediate"}}"#,
    );
    while node.step_index_builds(std::time::Duration::from_secs(5)) > 0 {}
    node
}


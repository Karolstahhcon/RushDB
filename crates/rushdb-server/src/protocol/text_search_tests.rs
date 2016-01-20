//! `query.text` reports the index's unapplied backlog and accepts the
//! all-terms and phrase modes; `rushdb_text_pending` covers every text index
//! in every tenancy mode.

use super::*;
use rushdb_protocol::{RequestContext, RequestEnvelope, StatementLanguage};

fn execute(node: &Node, scope: Option<&str>, operation: Operation) -> Response {
    ProtocolExecutor::new(node, scope.map(str::to_owned))
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: scope.map(str::to_owned),
                deadline_ms: None,
                ..Default::default()
            },
            operation,
        })
        .unwrap()
        .response
}

/// An `eventual` whole-record text index `name` on `Doc`, built.
fn eventual_index(node: &Node, scope: Option<&str>, name: &str) {
    execute(
        node,
        scope,
        Operation::Mutate {
            mutation: Mutation::Statement {
                language: StatementLanguage::Mongo,
                statement: serde_json::json!({"createIndex": {"collection": "Doc",
                    "property": "*", "kind": "text", "name": name,
                    "consistency": "eventual"}})
                .to_string(),
                parameters: None,
            },
        },
    );
    while node.step_index_builds(std::time::Duration::from_secs(5)) > 0 {}
}

fn doc(node: &Node, scope: Option<&str>, body: &str) {
    execute(
        node,
        scope,
        Operation::Mutate {
            mutation: Mutation::PutRecord {
                mode: PutMode::Create,
                id: None,
                label: Some("Doc".into()),
                properties: serde_json::json!({ "body": body }),
                vectors: Vec::new(),
                options: serde_json::json!({}),
            },
        },
    );
}

/// `(hits, pending)` of one text search.
fn search(node: &Node, query: &str, all_terms: bool, phrase: bool) -> (u64, u64) {
    let response = execute(
        node,
        None,
        Operation::Query {
            query: Query::Text {
                index: "docs".into(),
                query: query.into(),
                limit: 10,
                all_terms,
                phrase,
            },
        },
    );
    let Response::TextHits { total, pending, .. } = response else {
        panic!("expected text hits, got {response:?}");
    };
    (total, pending)
}

#[test]
fn eventual_search_reports_its_backlog() {
    let node = Node::bootstrap(&mut super::tests::config(false)).unwrap();
    eventual_index(&node, None, "docs");
    for body in ["quick brown fox", "quick red fox", "lazy brown dog"] {
        doc(&node, None, body);
    }
    // No applier has run: the search misses all three and says so.
    assert_eq!(search(&node, "fox", false, false), (0, 3));
    node.apply_text_indexes(std::time::Duration::from_secs(5));
    assert_eq!(search(&node, "fox", false, false), (2, 0));
}

#[test]
fn all_terms_and_phrase_modes_reach_the_index() {
    let node = Node::bootstrap(&mut super::tests::config(false)).unwrap();

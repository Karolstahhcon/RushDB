//! 2A — openCypher conformance subset. A curated, versioned corpus runs in CI;
//! every case is classified as supported or unsupported with a reason. There
//! are no silent skips: if a query's actual outcome diverges from its
//! classification, this test fails and forces a deliberate reclassification.

use rushdb_protocol::{
    Mutation, Operation, Query, RequestContext, RequestEnvelope, StatementLanguage,
};
use rushdb_server::{Node, ServerConfig};

struct Case {
    name: &'static str,
    statement: &'static str,
    /// `Some(reason)` = expected to FAIL (unsupported / upstream defect);
    /// `None` = expected to succeed.
    unsupported: Option<&'static str>,
}

fn bootstrap() -> Node {
    let mut cfg = ServerConfig {
        languages: rushdb_protocol::LanguageSet {
            cypher: true,
            ..Default::default()
        },
        ..Default::default()
    };
    Node::bootstrap(&mut cfg).unwrap()
}

fn run_cypher(node: &Node, statement: &str) -> Result<(), String> {
    // Graph-language writes require the write-authorized Mutation operation;
    // reads go through Query. Route by whether the statement contains a write
    // clause anywhere (a MATCH...CREATE pattern still writes).
    let is_write = ["CREATE", "MERGE", "DELETE", "DROP"]
        .iter()
        .any(|kw| statement.split_whitespace().any(|tok| tok.starts_with(kw)));
    let operation = if is_write {
        Operation::Mutate {
            mutation: Mutation::Statement {
                language: StatementLanguage::Cypher,
                statement: statement.to_string(),
                parameters: None,
            },
        }
    } else {

//! T-5b: cross-statement read-your-writes. A statement staged later in a
//! transaction must observe the writes staged by earlier statements, because
//! statement planning reads through the transaction overlay.

use rushdb_protocol::{
    Operation, Query, QueryDocument, RequestContext, RequestEnvelope, Response, StatementLanguage,
    TransactionMutation, TransactionOperation,
};
use rushdb_server::{Node, ServerConfig};

fn cypher_node() -> Node {
    let mut config = ServerConfig::default();
    config.languages.cypher = true;
    Node::bootstrap(&mut config).unwrap()
}

fn execute(node: &Node, operation: Operation) -> Response {
    rushdb_server::protocol::ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: 1,
            request_id: None,
            context: RequestContext::default(),
            operation,
        })
        .unwrap()
        .response
}

fn begin(node: &Node) -> String {
    let Response::TransactionBegun { transaction_id } = execute(
        node,
        Operation::Transaction {
            transaction: TransactionOperation::Begin {
                ttl_ms: None,
                autocommit: false,
            },
        },
    ) else {
        panic!("expected transaction begin");
    };
    transaction_id
}

fn stage_statement(node: &Node, tx: &str, statement: &str) {
    execute(
        node,
        Operation::Transaction {
            transaction: TransactionOperation::Stage {
                transaction_id: tx.to_string(),
                mutation: TransactionMutation::Statement {
                    language: StatementLanguage::Cypher,
                    statement: statement.to_string(),
                    parameters: None,
                },
            },
        },
    );
}

fn commit(node: &Node, tx: &str) {
    execute(
        node,
        Operation::Transaction {
            transaction: TransactionOperation::Commit {
                transaction_id: tx.to_string(),
            },
        },
    );
}

fn t5_records(node: &Node) -> Vec<serde_json::Value> {
    let Response::Records { records, .. } = execute(
        node,
        Operation::Query {
            query: Query::Records {
                query: QueryDocument {
                    labels: vec!["T5".into()],
                    ..Default::default()
                },
            },
        },
    ) else {
        panic!("expected records");

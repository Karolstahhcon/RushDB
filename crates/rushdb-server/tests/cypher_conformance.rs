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
        Operation::Query {
            query: Query::Statement {
                language: StatementLanguage::Cypher,
                statement: statement.to_string(),
                parameters: None,
            },
        }
    };
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("cypher-conf".into()),
        context: RequestContext::default(),
        operation,
    };
    rushdb_server::protocol::ProtocolExecutor::new(node, None)
        .execute(envelope)
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

#[expect(
    clippy::too_many_lines,
    reason = "one flat, auditable classification table is clearer than splitting it"
)]
fn corpus() -> Vec<Case> {
    vec![
        // ── supported core (must keep passing) ─────────────────────────────
        Case {
            name: "literal-expr",
            statement: "RETURN 1 + 1 AS n",
            unsupported: None,
        },
        Case {
            name: "create-node",
            statement: "CREATE (n:FACTION {name: 'Rebel'})",
            unsupported: None,
        },
        Case {
            name: "match-return",
            statement: "MATCH (n:FACTION) RETURN n",
            unsupported: None,
        },
        Case {
            name: "match-where-props",
            statement: "MATCH (n:FACTION {name: 'Rebel'}) RETURN n.name",
            unsupported: None,
        },
        Case {
            name: "match-rel-return",
            statement: "MATCH (a:FACTION)-[:OWNS]->(b:PLANET) RETURN b",
            unsupported: None,
        },
        Case {
            name: "order-limit",
            statement: "MATCH (n:FACTION) RETURN n ORDER BY n.name LIMIT 5",
            unsupported: None,
        },
        Case {
            name: "with-projection",
            statement: "MATCH (n:FACTION) WITH n.name AS name RETURN name",
            unsupported: None,
        },
        Case {
            name: "unwind-list",
            statement: "UNWIND [1,2,3] AS x RETURN x",
            unsupported: None,
        },
        Case {
            name: "aggregate-count",
            statement: "MATCH (n:FACTION) RETURN count(n) AS c",
            unsupported: None,
        },
        Case {
            name: "distinct-projection",
            statement: "MATCH (n:FACTION) RETURN DISTINCT n.name",
            unsupported: None,
        },
        Case {
            name: "optional-match",
            statement: "MATCH (n:FACTION) OPTIONAL MATCH (n)-[:OWNS]->(p) RETURN n",
            unsupported: None,
        },
        Case {
            name: "union",
            statement: "MATCH (n:FACTION) RETURN n.name UNION MATCH (p:PLANET) RETURN p.name",
            unsupported: None,
        },
        Case {
            name: "merge-node",
            statement: "MERGE (n:FACTION {name: 'Hutt'})",
            unsupported: None,

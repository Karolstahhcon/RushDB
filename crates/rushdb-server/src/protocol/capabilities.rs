//! Language and feature capability advertisement.

use rushdb_protocol::{LanguageSet, QueryLanguageCapability};

#[expect(
    clippy::too_many_lines,
    reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
)]
pub(crate) fn query_language_capabilities(enabled: LanguageSet) -> Vec<QueryLanguageCapability> {
    use rushdb_protocol::{
        LanguageStatus, ParameterStyle, QueryLanguageCapability, StatementAccess,
        StatementLanguage, StatementTransport,
    };
    use ParameterStyle::{Named, Positional};
    use StatementAccess::{ReadOnly, ReadWrite};
    use StatementLanguage::{Cypher, Gql, Gremlin, Mongo, Sql, SqlPgq};
    use StatementTransport::{Pgwire, ProtocolJson};

    let all = vec![
        QueryLanguageCapability {
            language: Sql,
            status: LanguageStatus::Stable,
            profile: "PostgreSQL-dialect relational SQL (DataFusion 54.1)".into(),
            implementation: "Apache DataFusion".into(),
            access: ReadWrite,
            parameter_styles: vec![Positional, Named],
            transports: vec![ProtocolJson, Pgwire],
            mutation_transports: vec![ProtocolJson, Pgwire],
            features: language_features(&[
                "select",
                "filter",
                "projection",
                "join",
                "cte",
                "window",
                "aggregation",
                "order",
                "pagination",
                "preparedStatements",
                "relationshipTables",
                "insert",
                "update",
                "delete",
                "atomicStatementCommit",
            ]),
            limitations: language_features(&[
                "recordTablesOnlyForMutation",
                "existingCatalogColumnsOnly",
                "snapshotRowLimit:100000",
            ]),
        },
        QueryLanguageCapability {
            language: Gql,
            status: LanguageStatus::ReleaseCandidate,
            profile: "RushDB GQL property-graph profile v1 (ISO/IEC 39075 syntax subset)".into(),
            implementation: "Grafeo 0.5.42".into(),
            access: ReadWrite,
            parameter_styles: vec![Named],
            transports: vec![ProtocolJson],
            mutation_transports: vec![ProtocolJson],
            features: graph_language_features(),
            limitations: gql_language_limitations(),
        },
        QueryLanguageCapability {
            language: Cypher,
            status: LanguageStatus::ReleaseCandidate,
            profile: "openCypher property-graph profile".into(),
            implementation: "Grafeo 0.5.42".into(),
            access: ReadWrite,
            parameter_styles: vec![Named],
            transports: vec![ProtocolJson],
            mutation_transports: vec![ProtocolJson],
            features: graph_language_features(),
            limitations: cypher_language_limitations(),
        },
        QueryLanguageCapability {
            language: Gremlin,
            status: LanguageStatus::Stable,
            profile: "Apache TinkerPop Gremlin traversal profile (text and GraphSON 3 bytecode)"
                .into(),
            implementation: "Grafeo 0.5.42 / gremlin-rs grammar".into(),
            access: ReadWrite,
            parameter_styles: vec![Named],
            transports: vec![ProtocolJson],
            mutation_transports: vec![ProtocolJson],
            features: {
                let mut features = graph_language_features();
                features.push("graphSon3Bytecode".into());
                features
            },
            limitations: language_features(&[
                "singlePrimaryLabelPerRecord",
                "statementLabelMutationUnavailable",
                "graphBinaryNotExposed",
            ]),
        },
        QueryLanguageCapability {
            language: SqlPgq,
            status: LanguageStatus::Stable,
            profile: "SQL:2023 GRAPH_TABLE read profile".into(),
            implementation: "Grafeo 0.5.42".into(),
            access: ReadOnly,
            parameter_styles: vec![Named],
            transports: vec![ProtocolJson],
            mutation_transports: vec![],
            features: language_features(&["graphTable", "match", "filter", "projection", "order"]),
            limitations: language_features(&["readOnly"]),
        },
        QueryLanguageCapability {
            language: Mongo,
            status: LanguageStatus::Stable,
            profile: "MongoDB query language (find/aggregate + CRUD + index)".into(),
            implementation: "rushdb-mongodb".into(),
            access: ReadWrite,
            parameter_styles: vec![Named],
            transports: vec![ProtocolJson],
            mutation_transports: vec![ProtocolJson],
            features: language_features(&[
                "find",
                "filter",
                "projection",
                "sort",
                "pagination",
                "aggregate",
                "insert",

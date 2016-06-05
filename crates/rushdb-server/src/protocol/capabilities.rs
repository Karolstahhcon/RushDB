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

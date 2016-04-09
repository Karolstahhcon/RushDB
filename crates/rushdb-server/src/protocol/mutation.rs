//! Protocol mutation handlers on an admitted execution context.

use super::{
    execution_failed, language_execution_failed, query_document_to_parser_input, response_u64,
    sql_failed, ErrorCode, Mutation, ProtocolError, ProtocolExecutor, PutMode, Response,
    StatementLanguage,
};

impl ProtocolExecutor<'_> {
    #[expect(
        clippy::too_many_lines,
        clippy::cast_possible_truncation,
        reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
    )]
    pub(super) fn mutate(
        &self,
        mutation: Mutation,
        is_cancelled: &dyn Fn() -> bool,
        execution_context: &mut rushdb_engine::ExecutionContext,
    ) -> Result<Response, ProtocolError> {
        match mutation {
            Mutation::GremlinBytecode { .. } => Err(super::bytecode::not_rewritten()),
            Mutation::Statement {
                language,
                statement,
                parameters,
            } => {
                if !self.node.languages().enabled(language) {
                    return Err(ProtocolError::new(
                        ErrorCode::LanguageDisabled,
                        format!("mutation language `{language:?}` is not enabled"),
                    ));
                }
                // Neo4j-style index DDL before the graph translator.
                if matches!(language, StatementLanguage::Cypher | StatementLanguage::Gql) {
                    if let Some(ddl) = rushdb_language::ddl::parse_index_ddl(&statement) {
                        return self.index_ddl(ddl, execution_context);
                    }
                }
                // SQL index DDL is intercepted before DataFusion (which has no
                // DDL): CREATE [VECTOR] INDEX / DROP INDEX -> native index ops.
                if language == StatementLanguage::Sql {
                    if let Some(response) =
                        self.sql_index_ddl(&statement, execution_context, is_cancelled)?
                    {
                        return Ok(response);
                    }
                }
                if language == StatementLanguage::Sql {
                    let result = rushdb_sql::execute_sql_mutation_in_view_with_budget(
                        &self.node.lmpg(),
                        std::sync::Arc::clone(execution_context.read_view()),
                        &statement,
                        parameters.as_ref(),
                        execution_context.remaining_budget(),
                        Some(self.node.text_indexes()),
                    )
                    .map_err(sql_failed)?;
                    let rows = result
                        .rows
                        .into_iter()
                        .map(|row| {
                            serde_json::Value::Object(row.into_iter().collect::<serde_json::Map<
                                String,
                                serde_json::Value,
                            >>(
                            ))
                        })
                        .collect::<Vec<_>>();
                    return Ok(Response::StatementMutation {
                        total: rows.len() as u64,
                        rows,
                        affected: result.affected,
                        commit_index: result.commit_index,
                        status: None,
                    });
                }
                let language = match language {
                    StatementLanguage::Gql => rushdb_language::QueryLanguage::Gql,
                    StatementLanguage::Cypher => rushdb_language::QueryLanguage::Cypher,
                    StatementLanguage::Gremlin => rushdb_language::QueryLanguage::Gremlin,
                    StatementLanguage::SqlPgq => {
                        return Err(ProtocolError::new(
                            ErrorCode::InvalidRequest,
                            "SQL/PGQ is a read-only profile",
                        ))
                    }
                    StatementLanguage::Sql => unreachable!(),
                    StatementLanguage::Mongo => {
                        // Index commands are handled at the engine level.
                        let doc: serde_json::Value =
                            serde_json::from_str(&statement).map_err(execution_failed)?;
                        if let Some(idx) = doc.get("createIndex") {
                            let label = idx.get("collection").and_then(|v| v.as_str()).ok_or_else(
                                || {
                                    ProtocolError::new(
                                        ErrorCode::InvalidRequest,
                                        "createIndex requires `collection`",
                                    )
                                },
                            )?;
                            let property = idx
                                .get("property")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ProtocolError::new(
                                        ErrorCode::InvalidRequest,
                                        "createIndex requires `property`",
                                    )
                                })?;
                            if idx.get("dimensions").is_none() {
                                let name = idx
                                    .get("name")
                                    .and_then(|v| v.as_str())
                                    .map_or_else(|| format!("{label}_{property}"), str::to_string);
                                // `"kind": "text"` (optionally with
                                // `"consistency": "immediate"`) declares a
                                // full-text index.
                                let kind = match idx.get("kind").and_then(|v| v.as_str()) {
                                    None | Some("ordered") => {
                                        rushdb_language::ddl::DdlKind::Ordered
                                    }
                                    Some("text") => rushdb_language::ddl::DdlKind::Fulltext {
                                        immediate: idx.get("consistency").and_then(|v| v.as_str())
                                            == Some("immediate"),
                                    },
                                    Some(other) => {
                                        return Err(ProtocolError::new(
                                            ErrorCode::InvalidRequest,
                                            format!("unsupported index kind `{other}`"),
                                        ))
                                    }
                                };

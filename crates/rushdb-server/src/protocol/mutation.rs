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
                                return self.create_declared_index(&name, label, property, kind);
                            }
                            let similarity = idx
                                .get("similarity")
                                .and_then(|v| v.as_str())
                                .unwrap_or("cosine")
                                .to_string();
                            let dims = idx
                                .get("dimensions")
                                .and_then(serde_json::Value::as_u64)
                                .ok_or_else(|| {
                                    ProtocolError::new(
                                        ErrorCode::InvalidRequest,
                                        "createIndex requires `dimensions`",
                                    )
                                })?;
                            let name = idx.get("name").and_then(|v| v.as_str()).map(str::to_string);
                            let index = self
                                .node
                                .create_vector_index_with_execution_context(
                                    execution_context,
                                    &rushdb_protocol::VectorIndexDefinition {
                                        label: label.to_string(),
                                        property: property.to_string(),
                                        similarity,
                                        dimensions: dims as u32,
                                        name,
                                        model: None,
                                        owner: None,
                                    },
                                )
                                .map_err(execution_failed)?;
                            return Ok(Response::StatementMutation {
                                total: 0,
                                rows: vec![],
                                affected: 1,
                                commit_index: self.node.lmpg().store.committed_version(),
                                status: Some(
                                    index
                                        .get("name")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                ),
                            });
                        }
                        if let Some(idx) = doc.get("dropIndex") {
                            let name =
                                idx.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                                    ProtocolError::new(
                                        ErrorCode::InvalidRequest,
                                        "dropIndex requires `name`",
                                    )
                                })?;
                            self.drop_any_index(name, execution_context)?;
                            return Ok(Response::StatementMutation {
                                total: 0,
                                rows: vec![],
                                affected: 1,
                                commit_index: self.node.lmpg().store.committed_version(),
                                status: None,
                            });
                        }
                        if let Some(idx) = doc.get("reIndex") {
                            let name =
                                idx.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                                    ProtocolError::new(
                                        ErrorCode::InvalidRequest,
                                        "reIndex requires `name`",
                                    )
                                })?;
                            self.node
                                .revalidate_vector_index_with_execution_context(
                                    execution_context,
                                    name,
                                )
                                .map_err(execution_failed)?;
                            return Ok(Response::StatementMutation {
                                total: 0,
                                rows: vec![],
                                affected: 1,
                                commit_index: self.node.lmpg().store.committed_version(),
                                status: None,
                            });
                        }
                        let cancelled =
                            || is_cancelled() || execution_context.check_deadline().is_err();
                        let affected = rushdb_mongodb::execute_mutation_in_view_with_cancel(
                            &self.node.lmpg(),
                            execution_context.read_view(),
                            &statement,
                            &cancelled,
                        )
                        .map_err(execution_failed)?;
                        return Ok(Response::StatementMutation {
                            total: 0,
                            rows: vec![],
                            affected,
                            commit_index: self.node.lmpg().store.committed_version(),
                            status: None,
                        });
                    }
                };
                execution_context
                    .check_deadline()
                    .map_err(|error| execution_failed(error.to_string()))?;
                let result = rushdb_language::execute_mutation_in_view_with_limits(
                    &self.node.lmpg(),
                    execution_context.read_view(),
                    language,
                    &statement,
                    parameters.as_ref(),
                    rushdb_language::MUTATION_MAX_NODES,
                    execution_context
                        .remaining_budget()
                        .unwrap_or(rushdb_language::MUTATION_MAX_BYTES)
                        .min(rushdb_language::MUTATION_MAX_BYTES),
                    &|| execution_context.check_active().is_err(),
                    Some(self.node.text_indexes()),
                )
                .map_err(language_execution_failed)?;
                execution_context
                    .check_deadline()
                    .map_err(|error| execution_failed(error.to_string()))?;
                Ok(Response::StatementMutation {
                    total: result.result.rows.len() as u64,
                    rows: result.result.rows,
                    affected: result.affected,
                    commit_index: result.commit_index,
                    status: result.result.status,
                })
            }
            Mutation::PutRecord {
                mode,

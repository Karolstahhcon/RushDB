//! Protocol transactions handlers on an admitted execution context.

use super::{
    execution_failed, is_label_less_container, response_u64, ErrorCode, IndexOperation, Ingest,
    ProtocolError, ProtocolExecutor, Response, TransactionOperation, TransactionStatus,
};

impl ProtocolExecutor<'_> {
    pub(super) fn ingest(
        &self,
        ingest: Ingest,
        execution_context: &mut rushdb_engine::ExecutionContext,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<Response, ProtocolError> {
        execution_context
            .check_deadline()
            .map_err(|error| execution_failed(error.to_string()))?;
        if is_cancelled() {
            return Err(ProtocolError::new(
                ErrorCode::Cancelled,
                "operation cancelled",
            ));
        }
        let Some((label, documents, options)) = Self::ingest_input(ingest)? else {
            return Ok(Response::Ingested {
                count: 0,
                relationships: 0,
                records: Vec::new(),
            });
        };
        self.ingest_documents(
            label.as_deref(),
            &documents,
            &options,
            execution_context,
            is_cancelled,
        )
    }

    /// An import as the label, the documents and the options the documents
    /// ingest path takes, validated; `None` for a CSV without rows.
    fn ingest_input(
        ingest: Ingest,
    ) -> Result<Option<(Option<String>, serde_json::Value, serde_json::Value)>, ProtocolError> {
        match ingest {
            Ingest::Documents {
                label,
                documents,
                options,
            } => {
                let label = label
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                rushdb_lmpg::import::ImportOptions::validate_json(&options)
                    .map_err(|error| ProtocolError::new(ErrorCode::InvalidRequest, error))?;
                if label.is_none() && !is_label_less_container(&documents) {
                    return Err(ProtocolError::new(
                        ErrorCode::InvalidRequest,
                        "label is required for a top-level array, an empty object, or an object containing primitive properties",
                    ));
                }
                Ok(Some((label, documents, options)))
            }
            Ingest::Csv {
                label,
                data,
                parse_config,
                options,
                vectors,
            } => {
                let label = label.trim();
                if label.is_empty() {
                    return Err(ProtocolError::new(
                        ErrorCode::InvalidRequest,
                        "label is required for CSV import",
                    ));
                }
                rushdb_lmpg::import::ImportOptions::validate_json(&options)
                    .map_err(|error| ProtocolError::new(ErrorCode::InvalidRequest, error))?;
                let rows = super::csv::csv_documents(&data, &parse_config, &options, vectors)?;
                if rows.is_empty() {
                    return Ok(None);
                }
                Ok(Some((
                    Some(label.to_owned()),
                    serde_json::Value::Array(rows),
                    options,
                )))
            }
        }
    }

    /// Stage an import into a transaction: its records and relationships
    /// become visible on commit.
    fn stage_ingest(
        &self,
        transaction_id: String,
        ingest: Ingest,
        execution_context: &mut rushdb_engine::ExecutionContext,
    ) -> Result<Response, ProtocolError> {
        let (count, relationships) = match Self::ingest_input(ingest)? {
            Some((label, documents, options)) => self
                .node
                .tx_stage_ingest(&transaction_id, label.as_deref(), &documents, &options)
                .map_err(|error| self.transaction_failed(&transaction_id, error))?,
            None => (0, 0),
        };
        execution_context
            .check_deadline()
            .map_err(|error| execution_failed(error.to_string()))?;
        Ok(Response::TransactionIngested {
            transaction_id,
            count,
            relationships,
        })
    }

    fn ingest_documents(
        &self,
        label: Option<&str>,
        documents: &serde_json::Value,
        options: &serde_json::Value,
        execution_context: &mut rushdb_engine::ExecutionContext,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<Response, ProtocolError> {
        let result = self
            .node
            .ingest_documents_with_execution_context(
                execution_context,
                label,
                documents,
                options,
                is_cancelled,
            )
            .map_err(|error| {
                if execution_context.check_deadline().is_err() {
                    execution_failed("execution deadline exceeded")
                } else if is_cancelled() {
                    ProtocolError::new(ErrorCode::Cancelled, "operation cancelled")
                } else {
                    execution_failed(error)
                }
            })?;
        // The engine reports ingested records as `data` only when the
        // caller requested them. `returnResult: false` (the default)
        // yields `data: true`, and a result set above the inline
        // reconstruction bound omits `data` entirely. Both are normal
        // success shapes and map to an empty record list; any other
        // shape is a malformed engine payload.
        let records = match result.get("data") {
            Some(serde_json::Value::Array(records)) => records.clone(),
            Some(serde_json::Value::Bool(_)) | None => Vec::new(),
            Some(_) => {
                return Err(execution_failed(
                    "engine ingest `data` must be an array or boolean",
                ))
            }
        };
        execution_context
            .check_deadline()
            .map_err(|error| execution_failed(error.to_string()))?;
        if is_cancelled() {
            return Err(ProtocolError::new(
                ErrorCode::Cancelled,
                "operation cancelled",
            ));
        }
        Ok(Response::Ingested {
            count: response_u64(&result, "count")?,
            relationships: response_u64(&result, "relationships")?,
            records,
        })
    }

    pub(super) fn index(
        &self,
        index: IndexOperation,
        execution_context: &mut rushdb_engine::ExecutionContext,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<Response, ProtocolError> {
        execution_context
            .check_deadline()
            .map_err(|error| execution_failed(error.to_string()))?;
        if is_cancelled() {
            return Err(ProtocolError::new(
                ErrorCode::Cancelled,
                "operation cancelled",
            ));
        }
        let response = match index {
            IndexOperation::Create { definition } => Response::Index {
                index: self
                    .node
                    .create_index(&definition)
                    .map_err(execution_failed)?,
            },
            IndexOperation::Drop { name } => {
                self.node.drop_index(&name).map_err(execution_failed)?;
                Response::Mutation { affected: 1 }
            }
            IndexOperation::CreateVector { definition } => {
                let index = self
                    .node
                    .create_vector_index_with_execution_context(execution_context, &definition)
                    .map_err(execution_failed)?;

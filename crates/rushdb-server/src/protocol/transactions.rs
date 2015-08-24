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

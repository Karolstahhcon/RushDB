//! Node transactions operations. Context entry points retain caller-owned
//! snapshot, tenant, cancellation, and budget semantics.

use super::{bytes_from_hex, OwnerKind, RequestScope};

impl RequestScope<'_> {
    // ---- cross-request transactions (document 07) ----------------------
    /// Begin a cross-service transaction with the default TTL.
    ///
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn tx_begin(&self) -> Result<String, String> {
        self.tx_begin_with(None, false)
    }

    /// Begin a cross-service transaction with an optional per-transaction TTL
    /// override and the autocommit-on-expiry flag.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn tx_begin_with(&self, ttl_ms: Option<u64>, autocommit: bool) -> Result<String, String> {
        self.tx_coordinator()?.begin_with(ttl_ms, autocommit)
    }

    pub(crate) fn tx_begin_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        ttl_ms: Option<u64>,
        autocommit: bool,
    ) -> Result<String, String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let result = self.tx_begin_with(ttl_ms, autocommit)?;
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        Ok(result)
    }

    /// Stage one typed protocol mutation onto a transaction.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    #[expect(
        clippy::too_many_lines,
        reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
    )]
    pub fn tx_stage(
        &self,
        tx_id: &str,
        mutation: &rushdb_protocol::TransactionMutation,
    ) -> Result<(), String> {
        use rushdb_lmpg::plan::FieldChange;
        let coordinator = self
            .existing_tx_coordinator()
            .ok_or("unknown transaction id")?;
        let record = |reference: &rushdb_protocol::RecordReference| -> Result<_, String> {
            let record_id = bytes_from_hex(&reference.id)?;
            Ok(rushdb_lmpg::RecordRef {
                record_id,
                ordinal: coordinator.resolve_record(tx_id, record_id)?,
            })
        };
        let batch_op = match mutation {
            rushdb_protocol::TransactionMutation::CreateRecord {
                label,
                id,
                properties,
                options,
            } => {
                rushdb_lmpg::import::ImportOptions::validate_json(options)?;
                let options = rushdb_lmpg::import::ImportOptions::from_json(options);
                let fields = properties
                    .as_object()
                    .ok_or("transaction properties must be an object")?
                    .iter()
                    .map(|(name, value)| {
                        let change = rushdb_lmpg::import::coerce_change(value, &options)
                            .unwrap_or_else(|| FieldChange::Set(value.clone()));
                        (name.clone(), change)
                    })
                    .collect();
                rushdb_lmpg::BatchOp::CreateRecord {
                    label: label.clone(),
                    explicit_id: id.as_deref().map(bytes_from_hex).transpose()?,
                    fields,
                }
            }
            rushdb_protocol::TransactionMutation::UpdateRecord {
                record: target,
                properties,
                replace,
            } => {
                let target = record(target)?;
                let mut fields = Self::props_from_properties(properties)?;
                if *replace {
                    // The record as this transaction sees it; the read joins
                    // the transaction's conflict checks.
                    let current = coordinator.get_record(tx_id, target.record_id)?;

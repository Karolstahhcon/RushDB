//! Node adapters operations. Context entry points retain caller-owned
//! snapshot, tenant, cancellation, and budget semantics.

use super::{
    access, bytes_from_hex, key, Arc, OwnerKind, RecordId, RelationshipId, RequestScope,
    P_FAMILY_SIG,
};

impl RequestScope<'_> {
    /// Execute a SQL statement, returning rows as JSON maps.
    ///
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn sql(&self, sql: &str) -> Result<Vec<rushdb_sql::Row>, String> {
        rushdb_sql::reset_work_counters();
        let result = rushdb_sql::execute_sql(&self.lmpg(), sql).map_err(|e| e.to_string());
        self.observe_sql_work();
        result
    }

    /// Execute a parameterized SQL statement, returning rows as JSON maps.
    ///
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn sql_with_parameters(
        &self,
        sql: &str,
        parameters: Option<&serde_json::Value>,
    ) -> Result<Vec<rushdb_sql::Row>, String> {
        self.sql_with_parameters_budget(sql, parameters, None)
    }

    /// Execute parameterized SQL with an optional response working-byte
    /// budget supplied by the caller's execution context.
    ///
    /// # Errors
    /// Returns a string when SQL parsing, execution, storage, or the response
    /// byte budget rejects the operation.
    pub fn sql_with_parameters_budget(
        &self,
        sql: &str,
        parameters: Option<&serde_json::Value>,
        max_result_bytes: Option<usize>,
    ) -> Result<Vec<rushdb_sql::Row>, String> {
        let snapshot = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        self.sql_with_parameters_budget_in_view(snapshot, sql, parameters, max_result_bytes)
    }

    /// Execute parameterized SQL against a caller-owned pinned view.
    pub(crate) fn sql_with_parameters_budget_in_view(
        &self,
        snapshot: Arc<rushdb_storage::ReadView>,
        sql: &str,
        parameters: Option<&serde_json::Value>,
        max_result_bytes: Option<usize>,
    ) -> Result<Vec<rushdb_sql::Row>, String> {
        self.sql_with_parameters_budget_in_view_cancellable(
            snapshot,
            sql,
            parameters,
            max_result_bytes,
            &|| false,
            None,
        )
        .map_err(|e| e.to_string())
    }

    /// Execute parameterized SQL against a caller-owned pinned view, polling
    /// `is_cancelled` between result batches so a long scan stops promptly.
    pub(crate) fn sql_with_parameters_budget_in_view_cancellable(
        &self,
        snapshot: Arc<rushdb_storage::ReadView>,
        sql: &str,
        parameters: Option<&serde_json::Value>,
        max_result_bytes: Option<usize>,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
        policy: Option<Arc<rushdb_types::security::ActivePolicy>>,
    ) -> Result<Vec<rushdb_sql::Row>, rushdb_sql::SqlError> {
        rushdb_sql::reset_work_counters();
        let result = match policy {
            // Under an enforced policy the statement sees only what it
            // allows.
            Some(policy) => rushdb_sql::execute_sql_under_policy_in_view(
                &self.lmpg(),
                snapshot,
                sql,
                parameters,
                max_result_bytes,
                is_cancelled,
                Some(self.text_indexes()),
                policy,
            ),
            None => rushdb_sql::execute_sql_with_parameters_budget_in_view_cancellable(
                &self.lmpg(),
                snapshot,
                sql,
                parameters,
                max_result_bytes,
                is_cancelled,
                Some(self.text_indexes()),
            ),
        };
        self.observe_sql_work();
        result
    }

    pub(super) fn observe_sql_work(&self) {
        let work = rushdb_sql::take_work_counters();
        self.observe_work("sql_scan", "candidate_rows", work.candidate_rows);
        self.observe_work("sql_scan", "reconstructed_rows", work.reconstructed_rows);
        self.observe_work("sql_scan", "filtered_rows", work.filtered_rows);
        self.observe_work("sql_scan", "posting_bytes", work.posting_bytes);
    }

    // ---- lexical ---------------------------------------------------------

    /// Run a lexical top-k search and resolve each hit to its public record id,
    /// resolving against the caller's admitted read view so hits are consistent
    /// with the rest of the transaction's snapshot. Also returns the index's
    /// changes not yet applied when the search ran.
    ///

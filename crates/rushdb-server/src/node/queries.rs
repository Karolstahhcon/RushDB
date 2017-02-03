//! Node queries operations. Context entry points retain caller-owned
//! snapshot, tenant, cancellation, and budget semantics.

use super::{
    cursor_resident_bytes, dashed_from_hex, Arc, Duration, Instant, PropertyTypeMap, RequestScope,
    ResultCursor, SearchRequest, MAX_CURSOR_PAGE_SIZE, MAX_RESIDENT_CURSOR_BYTES,
};

impl RequestScope<'_> {
    /// Execute a search request, returning the total match count and canonical records.
    ///
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn search(&self, req: &SearchRequest) -> Result<(u64, Vec<serde_json::Value>), String> {
        self.search_with_cancel(req, &|| false)
    }

    /// Execute a search request, polling `is_cancelled` for cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn search_with_cancel(
        &self,
        req: &SearchRequest,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<(u64, Vec<serde_json::Value>), String> {
        let view = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        self.search_in_view_with_cancel(req, &view, is_cancelled)
    }

    /// Execute a native search against a caller-owned execution context.
    ///
    /// The query uses the context's pinned view and remaining working byte
    /// budget; transport schedulers own admission around this synchronous
    /// execution, and response encoding remains the transport's responsibility.
    pub(crate) fn search_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        req: &SearchRequest,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<(u64, Vec<serde_json::Value>), String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let request = req.clone();
        let snapshot = Arc::clone(context.read_view());
        let work_budget = context.remaining_budget();
        let page = rushdb_query::find_records_in_view_with_budget(
            &self.lmpg(),
            &request,
            is_cancelled,
            snapshot,
            work_budget,
        )
        .map_err(|error| error.to_string())?;
        if is_cancelled() {
            return Err("operation cancelled".into());
        }
        let proptypes = self.proptypes_cached(context.read_view())?;
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let active = rushdb_authz::load_active_policy_in_view(context.read_view(), self.tenant())
            .map_err(|error| error.to_string())?;
        let authz = rushdb_lmpg::AuthorizedView::new(
            context.read_view().as_ref(),
            self.tenant(),
            active.as_ref(),
        );
        let records = page
            .records
            .into_iter()
            .filter_map(|record| authz.mask_record(record))
            .map(|record| Self::canonical_record_with(&record, &proptypes))
            .collect();
        Ok((page.total_matched, records))
    }

    /// Execute an aggregate against a caller-owned pinned read view.
    pub(crate) fn aggregate_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        req: &rushdb_query::AggregateRequest,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<(Vec<rushdb_query::GroupBucket>, u64), rushdb_query::QueryError> {
        use rushdb_query::QueryError;
        context
            .check_deadline()
            .map_err(|_| QueryError::Cancelled)?;
        let request = req.clone();
        let snapshot = Arc::clone(context.read_view());
        let result = rushdb_query::aggregate_with_cancel_in_view_with_budget(
            &self.lmpg(),
            &request,
            is_cancelled,
            snapshot,
            context.remaining_budget(),
        )?;
        if is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        context
            .check_deadline()
            .map_err(|_| QueryError::Cancelled)?;
        Ok(result)
    }

    pub(super) fn search_in_view_with_cancel(
        &self,
        req: &SearchRequest,
        view: &Arc<rushdb_storage::ReadView>,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<(u64, Vec<serde_json::Value>), String> {
        self.search_in_view_with_known_total(req, view, is_cancelled, None)
    }

    pub(super) fn search_in_view_with_budget(
        &self,
        req: &SearchRequest,
        view: &Arc<rushdb_storage::ReadView>,
        is_cancelled: &dyn Fn() -> bool,
        work_budget: Option<usize>,
    ) -> Result<(u64, Vec<serde_json::Value>), String> {
        let page = rushdb_query::find_records_in_view_with_budget(
            &self.lmpg(),
            req,
            is_cancelled,
            Arc::clone(view),
            work_budget,
        )
        .map_err(|error| error.to_string())?;
        let proptypes = self.proptypes_cached(view)?;
        let active = rushdb_authz::load_active_policy_in_view(view, self.tenant())
            .map_err(|error| error.to_string())?;
        let authz = rushdb_lmpg::AuthorizedView::new(view.as_ref(), self.tenant(), active.as_ref());
        let records = page
            .records
            .into_iter()
            .filter_map(|record| authz.mask_record(record))
            .map(|record| Self::canonical_record_with(&record, &proptypes))
            .collect();
        Ok((page.total_matched, records))
    }

    /// Page search with a caller-supplied total for this exact view and match
    /// set (E6 cursor continuation on a pinned view skips the exact-count
    /// rescan). `None` recomputes (ordinary search path).
    pub(super) fn search_in_view_with_known_total(
        &self,
        req: &SearchRequest,
        view: &Arc<rushdb_storage::ReadView>,
        is_cancelled: &dyn Fn() -> bool,
        known_total: Option<u64>,
    ) -> Result<(u64, Vec<serde_json::Value>), String> {
        let scoped = req.clone();
        // A finite native page uses the bounded execution path. Ordered pages
        // retain only their best `offset + limit` candidates; explicitly
        // unbounded requests retain the legacy complete-result contract.
        let page = if scoped.pagination.limit.is_some() {
            rushdb_query::find_records_streaming_with_total(
                &self.lmpg(),
                &scoped,
                is_cancelled,
                Arc::clone(view),
                known_total,
            )
        } else {
            if known_total.is_some() {
                return Err("known-total paging requires a finite limit".to_string());
            }
            rushdb_query::find_records_in_view(
                &self.lmpg(),
                &scoped,
                is_cancelled,
                Arc::clone(view),
            )
        }
        .map_err(|e| e.to_string())?;
        let proptypes = self.proptypes_cached(view)?;
        let active = rushdb_authz::load_active_policy_in_view(view, self.tenant())
            .map_err(|error| error.to_string())?;
        let authz = rushdb_lmpg::AuthorizedView::new(view.as_ref(), self.tenant(), active.as_ref());
        let records = page
            .records
            .into_iter()
            .filter_map(|record| authz.mask_record(record))
            .map(|r| Self::canonical_record_with(&r, &proptypes))
            .collect();
        Ok((page.total_matched, records))
    }

    /// Start a native Protocol v1 result cursor from a finite first page.
    /// The stored read view makes every subsequent page observe the same
    /// snapshot, and the cursor is bound to the current tenant keyspace.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    ///
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.

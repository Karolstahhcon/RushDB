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
    pub fn start_cursor(
        &self,
        search: &SearchRequest,
        requested_ttl_ms: u64,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<(u64, Vec<serde_json::Value>, Option<String>), String> {
        self.reap_expired_cursors();
        let limit = search
            .pagination
            .limit
            .ok_or_else(|| "cursor pagination requires a finite limit".to_string())?;
        if limit == 0 || limit > MAX_CURSOR_PAGE_SIZE {
            return Err(format!(
                "cursor page limit must be between 1 and {MAX_CURSOR_PAGE_SIZE}"
            ));
        }
        if requested_ttl_ms == 0 {
            return Err("cursor TTL must be greater than zero".to_string());
        }
        let ttl = Duration::from_millis(requested_ttl_ms).min(self.transactions.max_ttl());
        let view = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        self.start_cursor_on_view(search, ttl, is_cancelled, view, None, 0)
    }

    /// Start a cursor using a caller-owned execution context and pinned view.
    pub(crate) fn start_cursor_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        search: &SearchRequest,
        requested_ttl_ms: u64,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<(u64, Vec<serde_json::Value>, Option<String>), String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let limit = search
            .pagination
            .limit
            .ok_or_else(|| "cursor pagination requires a finite limit".to_string())?;
        if limit == 0 || limit > MAX_CURSOR_PAGE_SIZE {
            return Err(format!(
                "cursor page limit must be between 1 and {MAX_CURSOR_PAGE_SIZE}"
            ));
        }
        if requested_ttl_ms == 0 {
            return Err("cursor TTL must be greater than zero".to_string());
        }
        let ttl = Duration::from_millis(requested_ttl_ms).min(self.transactions.max_ttl());
        let result = self.start_cursor_on_view(
            search,
            ttl,
            is_cancelled,
            Arc::clone(context.read_view()),
            context.remaining_budget(),
            context.policy_generation(),
        );
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        result
    }

    pub(super) fn start_cursor_on_view(
        &self,
        search: &SearchRequest,
        ttl: Duration,
        is_cancelled: &dyn Fn() -> bool,
        view: Arc<rushdb_storage::ReadView>,
        work_budget: Option<usize>,
        policy_generation: u64,
    ) -> Result<(u64, Vec<serde_json::Value>, Option<String>), String> {
        self.reap_expired_cursors();
        let (total, records) = match work_budget {
            Some(budget) => {
                self.search_in_view_with_budget(search, &view, is_cancelled, Some(budget))?
            }
            None => self.search_in_view_with_cancel(search, &view, is_cancelled)?,
        };
        // Advance by the raw page size, not the post-policy record count. The
        // query consumes `min(limit, total - skip)` raw matches, but a
        // data-access policy can drop records from that page afterwards.
        // Advancing by the filtered count would re-serve or skip raw rows and
        // never reach `total`.
        let next_offset = match search.pagination.limit {
            Some(limit) => search.pagination.skip.saturating_add(limit).min(total),
            None => search.pagination.skip.saturating_add(records.len() as u64),
        };
        if next_offset >= total {
            return Ok((total, records, None));
        }
        let cursor_id = uuid::Uuid::now_v7().to_string();
        let retention_consumer = format!("cursor:{cursor_id}");
        let retention_floor = view.commit_index();
        let cursor = ResultCursor {
            tenant: self.tenant(),
            policy_generation,
            retention_consumer: retention_consumer.clone(),
            view,
            search: search.clone(),
            next_offset,
            total,
            expires_at: Instant::now() + ttl,
            _reservation: None,
        };
        let resident = cursor_resident_bytes(&cursor);
        let reservation = self
            .store
            .cache_budget()
            .map(|budget| {
                budget
                    .try_reserve(resident)
                    .map_err(|error| format!("cursor shared cache budget exhausted: {error}"))
            })
            .transpose()?;
        let mut cursors = self
            .cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut cursor_bytes = self
            .cursor_bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if resident > MAX_RESIDENT_CURSOR_BYTES
            || cursor_bytes.saturating_add(resident) > MAX_RESIDENT_CURSOR_BYTES
        {
            return Err("cursor resident byte budget exhausted".to_string());
        }
        let cursor = ResultCursor {
            _reservation: reservation,
            ..cursor
        };
        cursors.insert(cursor_id.clone(), cursor);
        *cursor_bytes = cursor_bytes.saturating_add(resident);
        self.retain_ephemeral_cursor(retention_consumer, retention_floor)?;
        Ok((total, records, Some(cursor_id)))
    }

    /// Continue a native cursor. Unknown, expired, and foreign-tenant ids are
    /// intentionally indistinguishable to avoid exposing cursor existence.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    ///
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    pub fn continue_cursor(
        &self,
        cursor_id: &str,
        limit: u64,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<(u64, Vec<serde_json::Value>, Option<String>), String> {
        self.continue_cursor_inner(cursor_id, limit, is_cancelled, None, None)
    }

    /// Continue a cursor using the current request's deadline and work budget.
    pub(crate) fn continue_cursor_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        cursor_id: &str,
        limit: u64,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<(u64, Vec<serde_json::Value>, Option<String>), String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let result = self.continue_cursor_inner(
            cursor_id,
            limit,
            is_cancelled,
            context.remaining_budget(),
            Some(context.policy_generation()),
        );
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        result
    }

    pub(super) fn continue_cursor_inner(
        &self,
        cursor_id: &str,
        limit: u64,
        is_cancelled: &dyn Fn() -> bool,
        work_budget: Option<usize>,
        expected_policy_generation: Option<u64>,
    ) -> Result<(u64, Vec<serde_json::Value>, Option<String>), String> {
        self.reap_expired_cursors();
        if limit == 0 || limit > MAX_CURSOR_PAGE_SIZE {
            return Err(format!(
                "cursor page limit must be between 1 and {MAX_CURSOR_PAGE_SIZE}"
            ));
        }
        let current_tenant = self.tenant();
        let cursor = {
            let mut cursors = self
                .cursors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match cursors.get(cursor_id) {
                Some(cursor)
                    if cursor.expires_at > Instant::now()
                        && cursor.tenant == current_tenant

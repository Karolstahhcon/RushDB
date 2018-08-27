//! Protocol introspection handlers on an admitted execution context.

use super::{
    engine_capabilities, execution_failed, query_document_to_parser_input,
    query_language_capabilities, response_array, Describe, ErrorCode, LabelDescription,
    ProtocolError, ProtocolExecutor, Response, SchemaDescription,
};

impl ProtocolExecutor<'_> {
    /// The facet match set of a query document: its labels and predicate,
    /// without projection, grouping, ordering or pagination.
    pub(super) fn facet_search(
        query: &rushdb_protocol::QueryDocument,
    ) -> Result<rushdb_query::SearchRequest, ProtocolError> {
        let document = serde_json::json!({"labels": query.labels, "where": query.predicate});
        rushdb_query::document::parse_document(&document)
            .map(|parsed| parsed.search)
            .map_err(|error| ProtocolError::new(ErrorCode::InvalidRequest, error.to_string()))
    }

    /// Match-set ordinals of a filtered query, or `None` when it has no
    /// predicate (the label catalogs answer it).
    fn facet_matches(
        &self,
        query: &rushdb_protocol::QueryDocument,
        is_cancelled: &dyn Fn() -> bool,
        execution_context: &rushdb_engine::ExecutionContext,
    ) -> Result<Option<Vec<u64>>, ProtocolError> {
        if query
            .predicate
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
        {
            return Ok(None);
        }
        self.node
            .facet_ordinals(
                execution_context.read_view(),
                &Self::facet_search(query)?,
                is_cancelled,
            )
            .map(Some)
            .map_err(execution_failed)
    }

    /// Record count per label over a query's match set: from the label
    /// counters when the query names labels only, else from `matched`.
    fn facet_label_counts(
        &self,
        query: &rushdb_protocol::QueryDocument,
        matched: Option<&[u64]>,
        is_cancelled: &dyn Fn() -> bool,
        execution_context: &rushdb_engine::ExecutionContext,
    ) -> Result<std::collections::BTreeMap<String, u64>, ProtocolError> {
        let view = execution_context.read_view();
        let Some(ords) = matched else {
            return Ok(self
                .node
                .label_counts_in_view(view)
                .map_err(execution_failed)?
                .into_iter()
                .filter(|(label, count)| *count > 0 && query.labels.contains(label))
                .collect());
        };
        self.node
            .facet_labels(view.as_ref(), ords, is_cancelled)
            .map_err(execution_failed)
    }

    /// [`Self::describe_with_access`] under the tenant's policy, read from
    /// the request's pinned view (the dispatcher passes the access it
    /// already built).
    #[cfg(test)]
    pub(super) fn describe(
        &self,
        describe: Describe,
        is_cancelled: &dyn Fn() -> bool,
        execution_context: &mut rushdb_engine::ExecutionContext,
    ) -> Result<Response, ProtocolError> {
        let active = rushdb_authz::load_active_policy_in_view(
            execution_context.read_view().as_ref(),
            execution_context.tenant(),
        )
        .map_err(|error| execution_failed(error.to_string()))?;
        let access = rushdb_lmpg::CatalogAccess::new(active.map(std::sync::Arc::new));
        self.describe_with_access(describe, &access, is_cancelled, execution_context)
    }

    /// A catalog read under `access`: over an enforced policy's allowed
    /// labels, visible properties and allowed relationship types only
    ///, so a hidden one is never named or counted.
    pub(super) fn describe_with_access(
        &self,
        describe: Describe,
        access: &rushdb_lmpg::CatalogAccess,
        is_cancelled: &dyn Fn() -> bool,
        execution_context: &mut rushdb_engine::ExecutionContext,
    ) -> Result<Response, ProtocolError> {
        execution_context
            .check_deadline()
            .map_err(|error| execution_failed(error.to_string()))?;
        // Narrow label-scoped reads to the allowed labels; when none is
        // left, the answer is empty without reading anything.
        let describe = match describe {
            Describe::Schema {
                query,
                detail,
                skip,
                limit,
            } => match self.visible_query(query, access, execution_context)? {
                Some(query) => Describe::Schema {
                    query,
                    detail,
                    skip,
                    limit,
                },
                None => {
                    return Ok(Response::Schema {
                        schema: SchemaDescription {
                            labels: Vec::new(),
                            properties: Vec::new(),
                            relationships: Vec::new(),
                        },
                    })
                }
            },
            Describe::Labels { query } => {
                match self.visible_query(query, access, execution_context)? {
                    Some(query) => Describe::Labels { query },
                    None => return Ok(Response::Labels { labels: Vec::new() }),
                }
            }
            Describe::Properties {
                query,
                detail,
                skip,
                limit,
            } => match self.visible_query(query, access, execution_context)? {
                Some(query) => Describe::Properties {
                    query,
                    detail,
                    skip,
                    limit,
                },
                None => {
                    return Ok(Response::Properties {
                        properties: Vec::new(),
                    })
                }
            },
            other => other,
        };
        let response = clip_introspection(
            self.describe_visible(describe, access, is_cancelled, execution_context)?,
            self.node.introspection_value_max_chars(),
        );
        if !access.enforced() {
            return Ok(response);
        }
        let label_counts: std::collections::BTreeMap<String, u64> = self
            .node
            .label_counts_in_view(execution_context.read_view())
            .map_err(execution_failed)?
            .into_iter()
            .collect();
        Ok(Self::mask_catalog(response, access, &label_counts))
    }

    /// A catalog query narrowed to `access`'s allowed labels: the requested
    /// ones that are allowed, or every allowed one. `None` when no label is
    /// left to read.
    fn visible_query(
        &self,
        mut query: rushdb_protocol::QueryDocument,
        access: &rushdb_lmpg::CatalogAccess,
        execution_context: &rushdb_engine::ExecutionContext,
    ) -> Result<Option<rushdb_protocol::QueryDocument>, ProtocolError> {
        if !access.enforced() {
            return Ok(Some(query));
        }
        if query.labels.is_empty() {
            query.labels = self
                .node
                .labels_in_view(execution_context.read_view())
                .map_err(execution_failed)?;
        }

//! Protocol queries handlers on an admitted execution context.

use super::{
    execution_failed, query_document_to_parser_input, query_failed, response_array, response_u64,
    values_range, ErrorCode, ProtocolError, ProtocolExecutor, QueryDocument, Response,
};
use rushdb_lmpg::CatalogAccess;
use rushdb_types::value_match::{TextMatch, ValueMatcher};

/// Rank matching values for a value search: descending owner count, ties by
/// the value's JSON text, so results are stable across reads.
fn rank_values(values: &mut [(serde_json::Value, u64)]) {
    values.sort_by(|a, b| {
        b.1.cmp(&a.1).then_with(|| {
            let left = serde_json::to_string(&a.0).unwrap_or_default();
            let right = serde_json::to_string(&b.0).unwrap_or_default();
            left.cmp(&right)
        })
    });
}

/// The engine's matcher for a wire value search.
fn value_matcher(search: &rushdb_protocol::ValueSearch) -> ValueMatcher {
    use rushdb_protocol::ValueMatch;
    let matching = match search.matching {
        ValueMatch::Contains => TextMatch::Contains,
        ValueMatch::Prefix => TextMatch::Prefix,
        ValueMatch::Exact => TextMatch::Exact,
        ValueMatch::Similar => TextMatch::Similar,
    };
    ValueMatcher::new(
        &search.text,
        matching,
        search.case_sensitive,
        search.min_similarity,
    )
}

/// A `similar` search ranks by similarity, then by the count order already
/// given; other searches keep it.
fn rank_by_similarity(
    search: &ValueMatcher,
    ranked: Vec<(serde_json::Value, u64)>,
) -> Vec<(serde_json::Value, u64)> {
    if search.matching() != TextMatch::Similar {
        return ranked;
    }
    let mut scored: Vec<(f64, (serde_json::Value, u64))> = ranked
        .into_iter()
        .map(|entry| (search.similarity(&entry.0), entry))
        .collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0));
    scored.into_iter().map(|(_, entry)| entry).collect()
}

/// A property-value response over ranked matches (CAT-2 `search`).
#[expect(
    clippy::cast_possible_truncation,
    reason = "limits are bounded request values"
)]
fn property_values_from_ranked(
    property: String,
    mode: rushdb_protocol::PropertyValuesMode,
    limit: Option<u64>,
    ranked: Vec<(serde_json::Value, u64)>,
) -> Response {
    use rushdb_protocol::PropertyValuesMode;
    let distinct_count = Some(ranked.len() as u64);
    let empty = |min, max| Response::PropertyValues {
        property: property.clone(),
        values: Vec::new(),
        distinct_count,
        min,
        max,
        truncated: false,
        counts: Vec::new(),
        search_missed: false,
    };
    match mode {
        PropertyValuesMode::Count => empty(None, None),
        PropertyValuesMode::Range => {
            let values: Vec<serde_json::Value> = ranked.into_iter().map(|(v, _)| v).collect();
            let (min, max) = values_range(&values);
            empty(min, max)
        }
        PropertyValuesMode::TopK => {
            let k = limit.unwrap_or(10).min(1000) as usize;
            let (values, counts) = ranked.into_iter().take(k).unzip();
            Response::PropertyValues {
                property,
                values,
                distinct_count,
                min: None,
                max: None,
                truncated: false,
                counts,
                search_missed: false,
            }
        }
        PropertyValuesMode::Values => {
            let n = limit.map_or(ranked.len(), |n| (n as usize).min(ranked.len()));
            let truncated = n < ranked.len();
            let values = ranked.into_iter().take(n).map(|(v, _)| v).collect();
            Response::PropertyValues {
                property,
                values,
                distinct_count,
                min: None,
                max: None,
                truncated,
                counts: Vec::new(),
                search_missed: false,
            }
        }
    }
}

impl ProtocolExecutor<'_> {
    pub(super) fn query_records_with_context(
        &self,
        query: &QueryDocument,
        is_cancelled: &dyn Fn() -> bool,
        mut execution_context: Option<&mut rushdb_engine::ExecutionContext>,
    ) -> Result<Response, ProtocolError> {
        if query.cursor_ttl_ms == Some(0) {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "cursorTtlMs must be greater than zero",
            ));
        }
        if query.cursor_ttl_ms.is_some() && query.limit.is_none() {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "cursor pagination requires a finite limit",
            ));
        }
        let document = query_document_to_parser_input(query);
        let mut parsed = rushdb_query::document::parse_document(&document)
            .map_err(|error| ProtocolError::new(ErrorCode::InvalidRequest, error.to_string()))?;
        if let Some(context) = execution_context.as_deref_mut() {
            parsed.search = self
                .node
                .authorize_search_in_view(context.read_view().as_ref(), &parsed.search)
                .map_err(|error| ProtocolError::new(ErrorCode::Unauthorized, error))?;
            if let Some(aggregate) = parsed.aggregate.as_mut() {
                *aggregate = self
                    .node
                    .authorize_aggregate_in_view(context.read_view().as_ref(), aggregate)
                    .map_err(|error| ProtocolError::new(ErrorCode::Unauthorized, error))?;
            }
        }
        if let Some(aggregate) = parsed.aggregate {
            if query.cursor_ttl_ms.is_some() {
                return Err(ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    "cursor pagination is available only for records queries",
                ));
            }
            let (buckets, matched) = if let Some(context) = execution_context.as_mut() {
                self.node
                    .aggregate_with_execution_context(context, &aggregate, is_cancelled)
                    .map_err(query_failed)?
            } else {
                rushdb_query::aggregate_with_cancel(&self.node.lmpg(), &aggregate, is_cancelled)
                    .map_err(query_failed)?
            };
            let records = if let Some(context) = execution_context.as_ref() {
                rushdb_query::aggregate_to_rows_in_view_with_budget(
                    &self.node.lmpg(),
                    context.read_view(),
                    &buckets,
                    &aggregate,
                    context.remaining_budget(),
                )
            } else {
                rushdb_query::aggregate_to_rows(&self.node.lmpg(), &buckets, &aggregate)
            }
            .map_err(query_failed)?;
            return Ok(Response::Rows {
                total: matched,
                rows: records,
            });
        }
        let (total, records, cursor_id) = match query.cursor_ttl_ms {
            Some(ttl) => {
                if let Some(context) = execution_context.as_mut() {
                    self.node.start_cursor_with_execution_context(
                        context,
                        &parsed.search,
                        ttl,
                        is_cancelled,
                    )
                } else {
                    self.node.start_cursor(&parsed.search, ttl, is_cancelled)
                }
            }
            None => {
                if let Some(context) = execution_context.as_mut() {
                    self.node
                        .search_with_execution_context(context, &parsed.search, is_cancelled)
                        .map(|(total, records)| (total, records, None))
                } else {
                    self.node
                        .search_with_cancel(&parsed.search, is_cancelled)
                        .map(|(total, records)| (total, records, None))
                }
            }
        }
        .map_err(execution_failed)?;
        Ok(Response::Records {
            records,
            total,
            cursor_id,
            vector_search: None,
            text_search: None,
            fusion: None,
        })
    }

    pub(super) fn query_cursor_with_context(
        &self,
        cursor_id: &str,
        limit: u64,
        is_cancelled: &dyn Fn() -> bool,
        context: &mut rushdb_engine::ExecutionContext,
    ) -> Result<Response, ProtocolError> {
        if limit == 0 || limit > 10_000 {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "cursor page limit must be between 1 and 10000",
            ));
        }
        let (total, records, cursor_id) = self
            .node
            .continue_cursor_with_execution_context(context, cursor_id, limit, is_cancelled)
            .map_err(|error| ProtocolError::new(ErrorCode::NotFound, error))?;
        Ok(Response::Records {
            records,
            total,
            cursor_id,
            vector_search: None,
            text_search: None,
            fusion: None,
        })
    }

    /// Intercept SQL index DDL: `CREATE VECTOR INDEX` / `DROP INDEX`. Returns
    /// `Ok(Some(response))` when the statement was DDL (handled), `Ok(None)`
    /// to fall through to `DataFusion`.
    #[expect(
        clippy::too_many_lines,
        reason = "DDL parsing and one atomic native index operation stay together at the protocol boundary"
    )]
    pub(super) fn sql_index_ddl(
        &self,
        sql: &str,
        execution_context: &mut rushdb_engine::ExecutionContext,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<Option<Response>, ProtocolError> {
        execution_context
            .check_deadline()
            .map_err(|error| execution_failed(error.to_string()))?;
        if is_cancelled() {
            return Err(ProtocolError::new(
                ErrorCode::Cancelled,
                "operation cancelled",
            ));
        }
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("CREATE VECTOR INDEX") {
            // Keep the caller's spelling for the durable identity while using
            // the uppercase copy only for case-insensitive grammar matching.
            let rest = &trimmed["CREATE VECTOR INDEX".len()..];
            let name = rest
                .split_whitespace()
                .next()
                .ok_or_else(|| {
                    ProtocolError::new(
                        ErrorCode::InvalidRequest,
                        "CREATE VECTOR INDEX requires a name",
                    )
                })?
                .to_string();
            // ... ON <label>(<property>) [SIMILARITY cosine|dot] [DIMENSIONS n]
            let on_idx = upper.find(" ON ").ok_or_else(|| {
                ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    "CREATE VECTOR INDEX requires `ON <label>(<property>)`",
                )
            })?;
            let on_clause = trimmed[on_idx + 4..].trim();
            let paren = on_clause.find('(').ok_or_else(|| {
                ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    "CREATE VECTOR INDEX requires `ON <label>(<property>)`",
                )
            })?;
            let label = on_clause[..paren].trim().to_string();
            let prop_end = on_clause.find(')').ok_or_else(|| {
                ProtocolError::new(ErrorCode::InvalidRequest, "missing `)` in property")
            })?;
            let property = on_clause[paren + 1..prop_end].trim().to_string();
            let similarity = if let Some(s) = upper.rfind(" SIMILARITY ") {
                trimmed[s + " SIMILARITY ".len()..]
                    .split_whitespace()
                    .next()
                    .unwrap_or("cosine")
                    .to_ascii_lowercase()
            } else {
                "cosine".to_string()
            };
            let dims = if let Some(d) = upper.rfind(" DIMENSIONS ") {
                trimmed[d + " DIMENSIONS ".len()..]
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u32>().ok())
                    .ok_or_else(|| {
                        ProtocolError::new(
                            ErrorCode::InvalidRequest,
                            "DIMENSIONS must be an integer",
                        )
                    })?
            } else {
                return Err(ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    "CREATE VECTOR INDEX requires `DIMENSIONS n`",
                ));
            };
            let index = self
                .node
                .create_vector_index_with_execution_context(
                    execution_context,
                    &rushdb_protocol::VectorIndexDefinition {
                        label,
                        property,
                        similarity,
                        dimensions: dims,
                        name: Some(name),
                        model: None,
                        owner: None,
                    },
                )
                .map_err(execution_failed)?;
            execution_context
                .check_deadline()
                .map_err(|error| execution_failed(error.to_string()))?;
            if is_cancelled() {
                return Err(ProtocolError::new(
                    ErrorCode::Cancelled,
                    "operation cancelled",
                ));
            }
            return Ok(Some(Response::StatementMutation {
                total: 0,
                rows: vec![],
                affected: 1,
                commit_index: self.node.lmpg().store.committed_version(),
                status: index.get("name").and_then(|v| v.as_str()).map(String::from),
            }));
        }
        if upper.starts_with("CREATE INDEX") {
            // CREATE INDEX <name> ON <label>(<property>): a declared ordered
            // index.
            let malformed = || {
                ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    "expected CREATE INDEX <name> ON <label>(<property>) [USING TEXT [IMMEDIATE]]",
                )
            };
            let name = trimmed
                .split_whitespace()
                .nth(2)
                .filter(|name| !name.eq_ignore_ascii_case("ON"))
                .ok_or_else(malformed)?;
            let on_idx = upper.find(" ON ").ok_or_else(malformed)?;
            let on_clause = trimmed[on_idx + 4..].trim();
            let paren = on_clause.find('(').ok_or_else(malformed)?;
            let close = on_clause.rfind(')').ok_or_else(malformed)?;
            let label = on_clause[..paren].trim().trim_matches('"');
            let property = on_clause[paren + 1..close].trim().trim_matches('"');
            if label.is_empty() || property.is_empty() || property.contains(',') {
                return Err(malformed());
            }
            // `… USING TEXT [IMMEDIATE]` declares a full-text index.
            let tail = on_clause[close + 1..].trim().to_ascii_uppercase();
            let kind = match tail.as_str() {
                "" => rushdb_language::ddl::DdlKind::Ordered,
                "USING TEXT" => rushdb_language::ddl::DdlKind::Fulltext { immediate: false },
                "USING TEXT IMMEDIATE" => {
                    rushdb_language::ddl::DdlKind::Fulltext { immediate: true }
                }
                _ => return Err(malformed()),
            };
            return self
                .create_declared_index(name, label, property, kind)
                .map(Some);
        }
        if upper.starts_with("DROP INDEX") {
            let name = trimmed
                .split_whitespace()
                .nth(2)
                .ok_or_else(|| {
                    ProtocolError::new(ErrorCode::InvalidRequest, "DROP INDEX requires a name")
                })?
                .to_string();
            self.drop_any_index(&name, execution_context)?;
            execution_context
                .check_deadline()
                .map_err(|error| execution_failed(error.to_string()))?;
            if is_cancelled() {
                return Err(ProtocolError::new(
                    ErrorCode::Cancelled,
                    "operation cancelled",
                ));
            }
            return Ok(Some(Response::StatementMutation {
                total: 0,
                rows: vec![],
                affected: 1,
                commit_index: self.node.lmpg().store.committed_version(),
                status: None,
            }));
        }
        Ok(None)
    }

    pub(super) fn query_relationships(
        &self,
        query: &rushdb_protocol::RelationshipQuery,
        context: &mut rushdb_engine::ExecutionContext,
    ) -> Result<Response, ProtocolError> {
        let result = self
            .node
            .scan_relationships_with_execution_context(context, query)
            .map_err(execution_failed)?;
        let relationships = response_array(&result, "data")?;
        // `total` is the filtered relationship count before pagination; the
        // page itself is `data`.
        let total = response_u64(&result, "total")?;
        Ok(Response::Relationships {
            relationships,
            total,
        })
    }

    #[expect(
        clippy::too_many_lines,
        clippy::too_many_arguments,
        clippy::cast_possible_truncation,
        reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
    )]
    pub(super) fn query_property_values(
        &self,
        property: &str,
        query: &QueryDocument,
        mode: Option<rushdb_protocol::PropertyValuesMode>,
        limit: Option<u64>,
        search: Option<&rushdb_protocol::ValueSearch>,
        is_cancelled: &dyn Fn() -> bool,
        context: &mut rushdb_engine::ExecutionContext,
        access: &CatalogAccess,
    ) -> Result<Response, ProtocolError> {
        use rushdb_protocol::PropertyValuesMode;
        let mode = mode.unwrap_or(PropertyValuesMode::Values);
        // A search that matches nothing may answer with the property's whole
        // domain instead, when it is small (`domainOnMiss`).
        if let Some((search, most)) = search.and_then(|search| {
            search.domain_on_miss.map(|most| {
                let mut plain = search.clone();
                plain.domain_on_miss = None;
                (plain, most.min(1000))
            })
        }) {
            let searched = self.query_property_values(
                property,
                query,
                Some(mode),
                limit,
                Some(&search),
                is_cancelled,
                context,
                access,
            )?;
            if !matches!(
                searched,
                Response::PropertyValues {
                    distinct_count: Some(0),
                    ..
                }
            ) {
                return Ok(searched);
            }
            let domain = self.query_property_values(
                property,
                query,
                Some(PropertyValuesMode::TopK),
                Some(most),
                None,
                is_cancelled,
                context,
                access,
            )?;
            return Ok(match domain {
                Response::PropertyValues {
                    property,
                    values,
                    distinct_count: Some(total),
                    counts,
                    ..
                } if total > 0 && total <= most => Response::PropertyValues {
                    property,
                    values,
                    distinct_count: Some(total),
                    min: None,
                    max: None,
                    truncated: false,
                    counts,
                    search_missed: true,
                },
                _ => searched,
            });
        }
        let property = self
            .node
            .resolve_property_name_in_view(context.read_view().as_ref(), property)
            .map_err(execution_failed)?;
        let matcher = search.map(value_matcher);
        let mut meter = self.node.working_set(is_cancelled, context.deadline());
        // The maintained-posting fast path below answers over *all* labels and
        // cannot subtract a data-access policy. When a policy is enforced, force
        // the match-set count below, which intersects the policy before computing
        // values. This keeps counts/range/top-k correct under `PolicyEnforced`
        // instead of leaking denied labels.
        // No predicate: the per-label dictionaries are the facet, under the
        // requested labels (all labels when none are given).
        let unfiltered = !access.enforced()
            && query
                .predicate
                .as_object()
                .is_some_and(serde_json::Map::is_empty);
        let unscoped = unfiltered && query.labels.is_empty();
        // A value search over an unfiltered match set is answered from the
        // value dictionaries: matching values with exact owner counts,
        // without reading records.
        if let (Some(search), true) = (matcher.as_ref(), unfiltered) {
            if let Some(ranked) = self
                .node
                .value_dictionary_in_view(
                    context.read_view().as_ref(),
                    &property,
                    &query.labels,
                    Some(search),
                    &mut meter,
                )
                .map_err(execution_failed)?
            {
                return Ok(property_values_from_ranked(
                    property,
                    mode,
                    limit,

//! Node relationships operations. Context entry points retain caller-owned
//! snapshot, tenant, cancellation, and budget semantics.

use super::{access, bytes_from_hex, Arc, IndexedRecord, RecordId, RequestScope};

/// Value set of a property (scalar -> [scalar]; array -> elements). A pure
/// helper, not tenant-dependent — kept as a free function rather than on
/// `RequestScope` so its own lifetime parameter can't collide with the
/// impl's request-scope lifetime.
pub(super) fn key_values<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Vec<&'a serde_json::Value> {
    let Some(v) = obj.get(key) else {
        return Vec::new();
    };
    match v {
        serde_json::Value::Array(items) => items.iter().collect(),
        other => vec![other],
    }
}

/// Reference `any(sourceValue IN targetValues)` — exact (type-strict)
/// overlap after toString bucketing pre-filters.
pub(super) fn values_overlap(a: &[&serde_json::Value], b: &[&serde_json::Value]) -> bool {
    a.iter().any(|av| b.iter().any(|bv| av == bv))
}

/// The one key join behind link, unlink and the join query: visit every
/// `(source, target)` pair whose key values overlap (type-strict), each pair
/// once, in source order. Targets are bucketed by the text of each key
/// value, so the cost is linear in the records plus the matches, never
/// sources × targets. `visit` returns `false` to stop early.
///
/// # Errors
/// Returns whatever `visit` returns.
pub(super) fn for_each_key_match(
    sources: &[IndexedRecord],
    targets: &[IndexedRecord],
    source_key: &str,
    target_key: &str,
    mut visit: impl FnMut(&IndexedRecord, &IndexedRecord) -> Result<bool, String>,
) -> Result<(), String> {
    let mut buckets: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for (index, (_, _, record)) in targets.iter().enumerate() {
        for value in key_values(record, target_key) {
            buckets.entry(value.to_string()).or_default().push(index);
        }
    }
    let mut seen = std::collections::HashSet::new();
    for source in sources {
        let source_values = key_values(&source.2, source_key);
        seen.clear();
        for value in &source_values {
            let Some(candidates) = buckets.get(&value.to_string()) else {
                continue;
            };
            for &index in candidates {
                let target = &targets[index];
                if seen.insert(index)
                    && values_overlap(&source_values, &key_values(&target.2, target_key))
                    && !visit(source, target)?
                {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

/// Commit `batch` as one atomic commit, unless it stages nothing.
fn run_nonempty(lmpg: &rushdb_lmpg::Lmpg, batch: rushdb_lmpg::BatchTx) -> Result<(), String> {
    if !batch.ops.is_empty() {
        lmpg.run_batch(batch).map_err(|e| e.to_reason())?;
    }
    Ok(())
}

/// Relationships an unlink deletes, collected against one snapshot and
/// deleted in one atomic commit (each at most once).
#[derive(Default)]
struct Doomed {
    seen: std::collections::HashSet<u64>,
    rels: Vec<rushdb_lmpg::RelationshipRef>,
}

impl Doomed {
    /// Every `rel_type` edge from `from` to `to`.
    fn edges_between(
        &mut self,
        snap: &rushdb_storage::ReadView,
        tenant: rushdb_types::ids::TenantId,
        from: u64,
        to: u64,
        rel_type: Option<&str>,
    ) -> Result<(), String> {
        for (_, other, rel_ord) in
            access::adj_out(snap, tenant, from, rel_type).map_err(|e| e.to_string())?
        {
            if other != to || !self.seen.insert(rel_ord) {
                continue;
            }
            if let Some(rh) =
                access::relationship_by_ord(snap, tenant, rel_ord).map_err(|e| e.to_string())?
            {
                self.rels.push(rushdb_lmpg::RelationshipRef {
                    relationship_id: rh.relationship_id,
                    ordinal: rh.ordinal,
                });
            }
        }
        Ok(())
    }

    /// Delete them all in one commit; the count deleted.
    fn delete(self, lmpg: &rushdb_lmpg::Lmpg) -> Result<usize, String> {
        let deleted = self.rels.len();
        let mut batch = lmpg.begin_batch();
        batch.ops.extend(
            self.rels
                .into_iter()
                .map(|rel| rushdb_lmpg::BatchOp::DeleteRelationship { rel }),
        );
        run_nonempty(lmpg, batch)?;
        Ok(deleted)
    }
}

impl RequestScope<'_> {
    #[cfg(test)]
    pub(super) fn relationship_exists(
        &self,
        src_ord: u64,
        rel_type: &str,
        tgt_ord: u64,
    ) -> Result<bool, String> {
        let snap = self.store.read_view().map_err(|e| e.to_string())?;
        access::edge_exists(&snap, self.tenant(), src_ord, rel_type, tgt_ord)
            .map_err(|e| e.to_string())
    }

    /// Execute the transport-neutral LMPG property join primitive.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn query_join_pairs(&self, query: &rushdb_protocol::JoinPairsQuery) -> Result<u64, String> {
        let snap = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        self.query_join_pairs_in_view(query, &snap, None, None)
    }

    /// Execute a property join against the request's pinned view and budget.
    /// The transport path uses this entry point so a join cannot silently
    /// switch snapshots or bypass the shared execution deadline.
    pub(crate) fn query_join_pairs_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        query: &rushdb_protocol::JoinPairsQuery,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<u64, String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let snap = Arc::clone(context.read_view());
        let result = self.query_join_pairs_in_view(query, &snap, Some(context), Some(is_cancelled));
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        result
    }

    pub(super) fn query_join_pairs_in_view(
        &self,
        query: &rushdb_protocol::JoinPairsQuery,
        snap: &Arc<rushdb_storage::ReadView>,
        mut context: Option<&mut rushdb_engine::ExecutionContext>,
        is_cancelled: Option<&dyn Fn() -> bool>,
    ) -> Result<u64, String> {
        let (left_property, right_property) = match &query.predicate {
            rushdb_protocol::JoinPredicate::PropertyOverlap {
                left_property,
                right_property,
            } => (left_property.as_str(), right_property.as_str()),
        };
        if query.left.label.trim().is_empty()
            || query.right.label.trim().is_empty()
            || left_property.trim().is_empty()
            || right_property.trim().is_empty()
        {
            return Err("join labels and properties must be non-empty".to_string());
        }
        if !query.left.filter.is_object() || !query.right.filter.is_object() {
            return Err("join filters must be objects".to_string());
        }
        if !(1..=10_000).contains(&query.limit) {
            return Err("join limit must be between 1 and 10000".to_string());
        }
        let left_filter = query
            .left
            .filter
            .as_object()
            .filter(|value| !value.is_empty());
        let right_filter = query
            .right
            .filter
            .as_object()
            .filter(|value| !value.is_empty());
        let sources = self.enumerate_records_in_view(
            &query.left.label,
            left_filter.map(|_| &query.left.filter),
            snap,
        )?;
        let targets = self.enumerate_records_in_view(
            &query.right.label,
            right_filter.map(|_| &query.right.filter),
            snap,
        )?;
        let mut pairs = std::collections::HashSet::<(u64, u64)>::new();
        let mut last_source = None;
        for_each_key_match(
            &sources,
            &targets,
            left_property,
            right_property,
            |(source_id, source_ord, _), (target_id, target_ord, _)| {
                if last_source != Some(*source_ord) {
                    last_source = Some(*source_ord);
                    if is_cancelled.is_some_and(|cancelled| cancelled()) {
                        return Err("operation cancelled".to_string());
                    }
                    if let Some(context) = context.as_deref_mut() {
                        context
                            .check_deadline()
                            .map_err(|error| error.to_string())?;
                        context
                            .charge_bytes(std::mem::size_of::<(u64, u64)>())
                            .map_err(|error| error.to_string())?;
                    }
                }
                if query.exclude_same_record && source_id == target_id {
                    return Ok(true);
                }
                pairs.insert((*source_ord, *target_ord));
                Ok(pairs.len() < query.limit as usize)
            },
        )?;
        Ok(pairs.len() as u64)
    }

    /// Match source and target record sets, then create typed relationships
    /// with relationship-owned properties.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    ///
    /// # Panics
    ///
    /// Panics if an internal invariant is violated, for example a poisoned lock.
    pub fn link_records(
        &self,
        command: &rushdb_protocol::RelationshipMutation,
    ) -> Result<serde_json::Value, String> {
        let snap = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        self.link_records_in_view(command, &snap)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
    )]
    pub(super) fn link_records_in_view(
        &self,
        command: &rushdb_protocol::RelationshipMutation,
        snap: &Arc<rushdb_storage::ReadView>,
    ) -> Result<serde_json::Value, String> {
        let rel_type = command.relation_type.clone();
        let direction = command.direction;
        let many_to_many = command.allow_cartesian;
        let props: Vec<(String, rushdb_lmpg::plan::FieldChange)> = command
            .properties
            .as_object()
            .map(|o| {
                o.iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            if v.is_null() {
                                rushdb_lmpg::plan::FieldChange::ExplicitNull
                            } else {
                                rushdb_lmpg::plan::FieldChange::Set(v.clone())
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let source_label = command.source.label.clone();
        let target_label = command.target.label.clone();
        let source_key = command.source.property.clone();
        let target_key = command.target.property.clone();
        let source_where = Some(&command.source.predicate);
        let target_where = Some(&command.target.predicate);

        let has_join_keys = source_key.is_some() && target_key.is_some();
        if !has_join_keys && !many_to_many {
            return Err(
                "source.property and target.property are required unless allowCartesian is true"
                    .to_string(),
            );
        }
        let source_has_where = command
            .source
            .predicate
            .as_object()
            .is_some_and(|value| !value.is_empty());
        let target_has_where = command
            .target
            .predicate
            .as_object()
            .is_some_and(|value| !value.is_empty());
        if many_to_many && !has_join_keys && (!source_has_where || !target_has_where) {
            return Err(
                "allowCartesian without join properties requires a non-empty `predicate` for both source and target"
                    .to_string(),
            );
        }

        let sources = self.enumerate_records_in_view(&source_label, source_where, snap)?;
        let targets = self.enumerate_records_in_view(&target_label, target_where, snap)?;

        // One atomic commit for every new edge: a conflict or an oversized
        // commit leaves nothing applied.
        let lmpg = self.lmpg();
        let mut batch = lmpg.begin_batch();
        let mut staged_pairs = std::collections::HashSet::new();
        let mut created = 0usize;
        let mut skipped_existing = 0usize;

        let mut stage = |(sid, sord, _): &IndexedRecord,
                         (tid, tord, _): &IndexedRecord|
         -> Result<bool, String> {
            // direction: out -> source->target ; in -> target->source
            let (a_ord, a_id, b_ord, b_id) = if direction == rushdb_protocol::Direction::In {
                (*tord, *tid, *sord, *sid)
            } else {
                (*sord, *sid, *tord, *tid)
            };
            // MERGE semantics: idempotent per pair, also within the call.
            if access::edge_exists(snap, self.tenant(), a_ord, &rel_type, b_ord)
                .map_err(|e| e.to_string())?
                || !staged_pairs.insert((a_ord, b_ord))
            {
                skipped_existing += 1;
                return Ok(true);
            }
            batch.ops.push(rushdb_lmpg::BatchOp::CreateRelationship {
                rel_type: rel_type.clone(),
                source: rushdb_lmpg::RecordRef {
                    record_id: a_id.0,
                    ordinal: a_ord,
                },
                target: rushdb_lmpg::RecordRef {
                    record_id: b_id.0,
                    ordinal: b_ord,
                },
                fields: props.clone(),
            });
            created += 1;
            Ok(true)
        };
        match (source_key.as_deref(), target_key.as_deref()) {
            (Some(source_key), Some(target_key)) => {
                for_each_key_match(&sources, &targets, source_key, target_key, &mut stage)?;
            }
            _ => {
                for source in &sources {
                    for target in &targets {
                        // self-exclusion when same label
                        if source_label != target_label || target.0 != source.0 {
                            stage(source, target)?;
                        }
                    }
                }
            }
        }
        run_nonempty(&lmpg, batch)?;

        Ok(serde_json::json!({
            "message": "Relations have been successfully created",
            "created": created,
            "skipped_existing": skipped_existing,
        }))
    }

    /// Query-based relationship linking using the caller-owned execution view.
    pub(crate) fn link_records_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        command: &rushdb_protocol::RelationshipMutation,
    ) -> Result<serde_json::Value, String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let result = self.link_records_in_view(command, context.read_view())?;
        Ok(result)
    }

    /// Match record pairs, then delete typed relationships between them.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn unlink_records(
        &self,
        command: &rushdb_protocol::RelationshipMutation,
    ) -> Result<serde_json::Value, String> {
        let snap = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        self.unlink_records_in_view(command, &snap)
    }

    pub(super) fn unlink_records_in_view(
        &self,
        command: &rushdb_protocol::RelationshipMutation,
        snap: &Arc<rushdb_storage::ReadView>,
    ) -> Result<serde_json::Value, String> {
        let rel_type = command.relation_type.clone();
        let source_label = command.source.label.clone();
        let target_label = command.target.label.clone();
        let source_key = command.source.property.clone();
        let target_key = command.target.property.clone();
        let many_to_many = command.allow_cartesian;

        let has_join_keys = source_key.is_some() && target_key.is_some();
        if !has_join_keys && !many_to_many {
            return Err(
                "source.property and target.property are required unless allowCartesian is true"
                    .to_string(),
            );
        }
        let source_has_where = command
            .source
            .predicate
            .as_object()
            .is_some_and(|value| !value.is_empty());
        let target_has_where = command
            .target
            .predicate
            .as_object()
            .is_some_and(|value| !value.is_empty());
        if many_to_many && !has_join_keys && (!source_has_where || !target_has_where) {
            return Err(
                "allowCartesian without join properties requires a non-empty `predicate` for both source and target"
                    .to_string(),
            );
        }

        let sources =
            self.enumerate_records_in_view(&source_label, Some(&command.source.predicate), snap)?;
        let targets =
            self.enumerate_records_in_view(&target_label, Some(&command.target.predicate), snap)?;
        let mut doomed = Doomed::default();
        let mut doom =
            |(_, sord, _): &IndexedRecord, (_, tord, _): &IndexedRecord| -> Result<bool, String> {
                // `direction` names which endpoint the edge leaves, as in link.
                let (from, to) = directed(command.direction, *sord, *tord);
                doomed.edges_between(snap, self.tenant(), from, to, Some(&rel_type))?;
                Ok(true)
            };
        match (source_key.as_deref(), target_key.as_deref()) {
            (Some(source_key), Some(target_key)) => {
                for_each_key_match(&sources, &targets, source_key, target_key, &mut doom)?;
            }
            _ => {
                for source in &sources {
                    for target in &targets {
                        if source_label != target_label || target.0 != source.0 {
                            doom(source, target)?;
                        }
                    }
                }

//! Node vectors operations. Context entry points retain caller-owned
//! snapshot, tenant, cancellation, and budget semantics.

use super::{
    access, append_bounded_json_response, key, normalize_vector_owner, similarity_from_str,
    validate_index_alias, Arc, HashSet, Node, OwnerKind, RequestScope, P_FAMILY_SIG,
};

impl Node {
    /// Whether every configured vector index has caught up with its source
    /// records. Used by `/ready` when `require_index_warmup` is set so traffic
    /// is not routed to a node whose ANN indexes are still warming.
    ///
    /// Multitenant mode reports `true`: there is no request scope on the
    /// readiness path, so a cross-tenant scan is neither possible nor desired.
    /// A read failure fails closed (`false`).
    #[must_use]
    pub fn indexes_warmed(&self) -> bool {
        if self.multitenant {
            return true;
        }
        let scope = RequestScope::for_tenant(self, self.base_tenant_id(), None);
        match scope.vector_indexes_all() {
            Ok(indexes) => indexes.iter().all(|index| {
                // `ready` (all bindings current) and `awaiting_vectors` (no
                // source values yet) are both settled; `behind` is not.
                index.get("status").and_then(serde_json::Value::as_str) != Some("behind")
            }),
            Err(_) => false,
        }
    }

    /// Tenant-explicit metadata write for callers that must not depend on a
    /// per-request scope (for example global retention state persisted by a
    /// background worker that has no single "current" tenant). Takes the
    /// tenant as an explicit argument rather than through `RequestScope`, so
    /// it stays callable from genuinely tenant-free maintenance paths.
    pub(super) fn put_meta_blob_for(
        &self,
        tenant: rushdb_types::ids::TenantId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), String> {
        let blob = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
        let k = key.to_string();
        let outcome = self
            .store
            .commit_for_tenant(&tenant, uuid::Uuid::now_v7(), move |_ci, _txn| {
                #[derive(serde::Serialize, serde::Deserialize)]
                struct P {}
                let batch = rushdb_types::command::CommitBatch {
                    command_version: rushdb_types::command::COMMAND_VERSION,
                    tenant,
                    transaction_id: String::new(),
                    commit_key: uuid::Uuid::nil(),
                    start_index: 0,
                    isolation: rushdb_types::command::IsolationMode::Snapshot,
                    preconditions: vec![],
                    mutations: vec![rushdb_types::command::Mutation::SetMetaBlob {
                        key: k.clone(),
                        value: blob,
                    }],
                };
                Ok((batch, P {}))
            })
            .map_err(|e| e.to_string())?;
        match outcome {
            rushdb_storage::CommitOutcome::Committed { .. } => Ok(()),
            rushdb_storage::CommitOutcome::Conflicted(r) => Err(r),
        }
    }
}

impl RequestScope<'_> {
    // ---- AI data-plane surface (embedding-index registry) ----------------
    // The DB owns vector storage/index metadata and exact search. Embedding
    // generation, NL->query, and schema-gen are the NestJS control plane's
    // responsibility; the DB never calls a model.

    pub(super) fn vector_index_key(&self, alias: &str) -> String {
        format!(
            "{}{alias}",
            rushdb_types::keys::vector_index_meta_prefix(self.tenant())
        )
    }

    pub(super) fn put_meta_blob(&self, key: &str, value: &serde_json::Value) -> Result<(), String> {
        let tenant = self.tenant();
        self.put_meta_blob_for(tenant, key, value)
    }

    pub(super) fn vector_indexes_all_in_view(
        &self,
        snap: &Arc<rushdb_storage::ReadView>,
    ) -> Result<Vec<serde_json::Value>, String> {
        let prefix = rushdb_types::keys::vector_index_meta_prefix(self.tenant()).into_bytes();
        let mut out = Vec::new();
        for (_, v) in snap.range_prefix(&prefix).map_err(|e| e.to_string())? {
            let mut row = serde_json::from_slice::<serde_json::Value>(&v)
                .map_err(|e| format!("corrupt vector index: {e}"))?;
            let alias = row
                .get("name")
                .and_then(|value| value.as_str())
                .ok_or("corrupt vector index: missing name")?;
            // Index metadata is durable, but readiness is derived from the
            // canonical bindings and source records at the read snapshot. Do
            // not persist a mutable "ready" bit that can survive a source
            // update or restart incorrectly.
            let stats = self.vector_index_stats_in_view(alias, snap)?;
            row["status"] = stats["status"].clone();
            row["indexedRecords"] = stats["indexedRecords"].clone();
            row["totalRecords"] = stats["totalRecords"].clone();
            row["unindexedRecords"] = stats["unindexedRecords"].clone();
            row["staleBindings"] = stats["staleBindings"].clone();
            row["retiredBindings"] = stats["retiredBindings"].clone();
            row["sourceTrackedBindings"] = stats["sourceTrackedBindings"].clone();
            row["externalBindings"] = stats["externalBindings"].clone();
            out.push(row);
        }
        Ok(out)
    }

    pub(super) fn vector_indexes_all(&self) -> Result<Vec<serde_json::Value>, String> {
        let snap = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        self.vector_indexes_all_in_view(&snap)
    }

    /// Create or resolve a vector index over a record property.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    #[expect(
        clippy::too_many_lines,
        reason = "index definition validation, metadata publication, and retention acknowledgement form one atomic lifecycle"
    )]
    pub fn create_vector_index(
        &self,
        definition: &rushdb_protocol::VectorIndexDefinition,
    ) -> Result<serde_json::Value, String> {
        let property = definition.property.clone();
        let label = definition.label.clone();
        let similarity = definition.similarity.clone();
        let dims = definition.dimensions;
        if label.trim().is_empty() || property.trim().is_empty() || dims == 0 {
            return Err(
                "vector index label/property must be set and dimensions must be positive".into(),
            );
        }

        let sim = similarity_from_str(&similarity)?;
        let owner = normalize_vector_owner(definition.owner.as_deref())?;
        // Ensure the owner-scoped property family exists so external vectors
        // can be bound and the backfill helper can resolve the property id.
        if owner == "relationship" {
            self.lmpg()
                .ensure_relationship_family(&property)
                .map_err(|e| e.to_reason())?;
        } else {
            self.lmpg()
                .ensure_record_family(&property)
                .map_err(|e| e.to_reason())?;
        }
        // Stable opaque name: the caller's `name` when provided, else a
        // deterministic alias required by Core's current `emb__...` contract.
        // The engine
        // carries vectors only; label/property/similarity/dims live in the
        // metadata record below, not in the name.
        let derived = rushdb_lmpg::import::external_space_alias(
            &label,
            &property,
            &similarity,
            dims as usize,
        );
        let name = match &definition.name {
            Some(explicit) => {
                validate_index_alias(explicit, &derived)?;
                explicit.clone()
            }
            None if self.require_vector_index_name => {
                return Err(
                    "vector index requires an explicit `name`; the derived alias cannot \
                     distinguish two same-shape embedding models \
                     (resources.require-vector-index-name is on)"
                        .into(),
                );
            }
            None => derived,
        };
        // A name's definition is immutable: a different model would mix
        // incomparable vector spaces, and a different label, property or
        // owner would silently repoint the index. Re-declaring the same
        // definition is a no-op that keeps the generation, so in-flight
        // embedding jobs stay valid.
        let mut unchanged = None;
        if let Some(raw) = self
            .store
            .try_get(self.vector_index_key(&name).as_bytes())
            .map_err(|e| e.to_string())?
        {
            let existing = serde_json::from_slice::<serde_json::Value>(&raw)
                .map_err(|e| format!("corrupt vector index: {e}"))?;
            let existing_model = existing.get("model").and_then(|v| v.as_str());
            if existing_model != definition.model.as_deref() {
                return Err(format!(
                    "vector index `{name}` already exists with a different model \
                     ({existing_model:?} != {:?})",
                    definition.model
                ));
            }
            for (field, requested) in [
                ("label", label.as_str()),
                ("property", property.as_str()),
                ("owner", owner),
            ] {
                let stored = existing.get(field).and_then(|v| v.as_str());
                if stored != Some(requested) {
                    return Err(format!(
                        "vector index `{name}` already exists for a different {field} \
                         ({stored:?} != {requested:?}); drop it first"
                    ));
                }
            }
            let same_shape = existing
                .get("dimensions")
                .and_then(serde_json::Value::as_u64)
                == Some(u64::from(dims))
                && existing.get("similarity").and_then(|v| v.as_str()) == Some(similarity.as_str());
            if same_shape {
                unchanged = Some(existing);
            }
        }
        self.vector_engine()?
            .ensure_space_similarity(&name, dims, sim)
            .map_err(|e| e.to_string())?;

        let vectors = self.vector_engine()?;
        let space = vectors
            .lookup_space_by_alias(&name)
            .map_err(|e| e.to_string())?;
        let ready = match space {
            Some(sp) => !vectors
                .bindings_of(sp.space_id)
                .map_err(|e| e.to_string())?
                .is_empty(),
            None => false,
        };
        let status = if ready { "ready" } else { "awaiting_vectors" };
        let name_source = if definition.name.is_some() {
            "explicit"
        } else {
            "derived"
        };
        let row = if let Some(existing) = unchanged {
            existing
        } else {
            let row = serde_json::json!({
                "id": name,
                "name": name,
                "label": label,
                "property": property,
                "similarity": similarity,
                "dimensions": dims,
                "status": status,
                "model": definition.model,
                "nameSource": name_source,
                "owner": owner,
                "generation": uuid::Uuid::now_v7().to_string(),
            });
            self.put_meta_blob(&self.vector_index_key(&name), &row)?;
            row
        };
        // A re-declaration can happen after records were added or bindings
        // became stale. Reconcile the published metadata with the same live
        // coverage calculation exposed by `vector_index_stats`, rather than
        // treating the presence of one binding as proof of readiness.
        let live_status = self
            .vector_index_stats(&name)?
            .get("status")
            .and_then(|value| value.as_str())
            .map(str::to_owned);
        if let Some(live_status) = live_status {
            if row.get("status").and_then(|value| value.as_str()) != Some(live_status.as_str()) {
                let mut reconciled = row;
                reconciled["status"] = serde_json::Value::String(live_status);
                self.put_meta_blob(&self.vector_index_key(&name), &reconciled)?;
                return Ok(reconciled);
            }
        }
        Ok(row)
    }

    pub(crate) fn create_vector_index_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        definition: &rushdb_protocol::VectorIndexDefinition,
    ) -> Result<serde_json::Value, String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let result = self.create_vector_index(definition)?;
        Ok(result)
    }

    /// Reconcile a managed vector index's binding statuses against live data:
    /// records whose source occurrence is gone become `Retired`, changed values
    /// become `Stale`. Core calls this for managed indexes during schema
    /// recalculation so a stale `ready` index reports accurate counts and can
    /// be returned to `pending` for re-embedding.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn revalidate_vector_index(&self, id: &str) -> Result<serde_json::Value, String> {
        let vectors = self.vector_engine()?;
        // Status reconciliation is the durable, primary effect. The ANN
        // rebuild is a derived-cache warmup: a build failure must not fail the
        // operation after the binding statuses have already been committed, and
        // the index is rebuilt lazily on the next ANN search. Surface the
        // failure in the response instead of returning an error over a partial
        // mutation.
        let (stale, retired) = vectors.revalidate_bindings(id).map_err(|e| e.to_string())?;
        let (ann_entries, ann_rebuild_error) = match vectors.build_ann(id) {
            Ok(generation) => (Some(generation.entries.len()), None),
            Err(error) => (None, Some(error)),
        };
        let snap = self.store.read_view().map_err(|e| e.to_string())?;
        let space = vectors
            .lookup_space_in_view(&snap, id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "index space missing".to_string())?;
        let ann_lifecycle = vectors
            .ann_lifecycle_in_view(&snap, &space)
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "stale": stale,
            "retired": retired,
            "annEntries": ann_entries,
            "annLifecycle": ann_lifecycle,
            "annRebuildError": ann_rebuild_error,
        }))
    }

    pub(crate) fn revalidate_vector_index_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        id: &str,
    ) -> Result<serde_json::Value, String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let result = self.revalidate_vector_index(id)?;
        Ok(result)
    }

    /// List vector indexes in the active scope.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn list_vector_indexes(&self) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({ "data": self.vector_indexes_all()? }))
    }

    pub(crate) fn list_vector_indexes_in_view(
        &self,
        snap: &Arc<rushdb_storage::ReadView>,
    ) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "data": self.vector_indexes_all_in_view(snap)?
        }))
    }

    /// Remove bindings, the vector-space alias, and its registry entry.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn drop_vector_index(&self, id: &str) -> Result<serde_json::Value, String> {
        let snap = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        if snap
            .get(self.vector_index_key(id).as_bytes())
            .map_err(|e| e.to_string())?
            .is_none()
        {
            return Err("index not found".to_string());
        }
        let cleared = self
            .vector_engine()?
            .clear_space(id)
            .map_err(|e| e.to_string())?;
        // remove the registry row
        let k = self.vector_index_key(id);
        let tenant = self.tenant();
        let outcome = self
            .store
            .commit_for_tenant(&tenant, uuid::Uuid::now_v7(), move |_ci, _txn| {
                #[derive(serde::Serialize, serde::Deserialize)]
                struct P {}
                let batch = rushdb_types::command::CommitBatch {
                    command_version: rushdb_types::command::COMMAND_VERSION,
                    tenant,
                    transaction_id: String::new(),
                    commit_key: uuid::Uuid::nil(),
                    start_index: 0,
                    isolation: rushdb_types::command::IsolationMode::Snapshot,
                    preconditions: vec![],
                    mutations: vec![rushdb_types::command::Mutation::DeleteMetaBlob { key: k }],
                };
                Ok((batch, P {}))
            })
            .map_err(|e| e.to_string())?;
        match outcome {
            rushdb_storage::CommitOutcome::Committed { .. } => {}
            rushdb_storage::CommitOutcome::Conflicted(r) => return Err(r),
        }
        Ok(serde_json::json!({ "message": "Index deleted", "cleared": cleared }))
    }

    pub(crate) fn drop_vector_index_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        id: &str,
    ) -> Result<serde_json::Value, String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let result = self.drop_vector_index(id)?;
        Ok(result)
    }

    /// Upsert caller-supplied vectors into an index.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn upsert_vector_index(
        &self,
        id: &str,
        items: &[rushdb_protocol::VectorItem],
    ) -> Result<serde_json::Value, String> {
        let snap = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        let raw = snap
            .get(self.vector_index_key(id).as_bytes())
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "index not found".to_string())?;
        let row: serde_json::Value = serde_json::from_slice(&raw)
            .map_err(|error| format!("corrupt vector index metadata: {error}"))?;
        let property = row
            .get("property")
            .and_then(|x| x.as_str())
            .ok_or_else(|| "corrupt vector index metadata: missing property".to_string())?
            .to_string();
        let dimensions = row
            .get("dimensions")
            .and_then(serde_json::Value::as_u64)
            .and_then(|d| u32::try_from(d).ok())
            .ok_or_else(|| "corrupt vector index metadata: invalid dimensions".to_string())?;
        let actual_generation = row
            .get("generation")
            .and_then(|value| value.as_str())
            .map(str::to_string);

        // Validate the batch identity before publishing any binding. A stale
        // completion in the same request must not leave earlier items
        // partially attached to the current generation.
        for item in items {
            if let Some(expected_generation) = item.index_generation.as_deref() {
                let Some(actual_generation) = actual_generation.as_deref() else {
                    return Err("vector index metadata has no generation".into());
                };
                if expected_generation != actual_generation {
                    return Err(format!(
                        "stale vector index generation (expected `{expected_generation}`, current `{actual_generation}`)"
                    ));
                }
            }
            if u32::try_from(item.vector.len()).ok() != Some(dimensions) {
                return Err(format!(
                    "vector dimension {} does not match index dimension {dimensions}",
                    item.vector.len()
                ));
            }
        }

        let lmpg = self.lmpg();
        let owner = row
            .get("owner")
            .and_then(|v| v.as_str())
            .unwrap_or("record");
        if owner == "relationship" {
            lmpg.ensure_relationship_family(&property)
                .map_err(|e| e.to_reason())?;
        } else {
            lmpg.ensure_record_family(&property)
                .map_err(|e| e.to_reason())?;
        }
        let mut updated = 0usize;
        // Untracked record vectors (the common backfill shape) bind in one
        // commit; source-hash-guarded and relationship items keep their
        // per-item guarded path.
        let batched: Vec<(&str, &[f32])> = items
            .iter()
            .filter(|item| owner != "relationship" && item.source_hash.is_none())
            .map(|item| (item.record_id.as_str(), item.vector.as_slice()))
            .collect();
        if !batched.is_empty() {
            updated += self
                .bind_field_vectors_external(id, &property, &batched)?
                .len();
        }
        for item in items {
            if owner != "relationship" && item.source_hash.is_none() {
                continue;
            }
            if owner == "relationship" {
                if let Some(expected_hash) = item.source_hash {
                    self.bind_relationship_vector_if_source_hash(
                        id,
                        &item.record_id,
                        &property,

//! Node records operations. Context entry points retain caller-owned
//! snapshot, tenant, cancellation, and budget semantics.

use std::sync::Arc;

use rushdb_lmpg::reader::try_access as access;
use rushdb_types::ids::RecordId;

use super::{bytes_from_hex, hex_simple, IndexedRecord, RequestScope};

impl RequestScope<'_> {
    // ---- record mutation ------------------------------------------------

    /// Parse a canonical record mutation into a BFS plan.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn create_record(
        &self,
        label: &str,
        properties: &serde_json::Value,
        vectors: &[serde_json::Value],
        options: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        use rushdb_lmpg::import::{
            import_bfs, BfsPlan, BfsRecord, ImportOptions, InlineVectorEntry,
        };
        ImportOptions::validate_json(options)?;
        let options = ImportOptions::from_json(options);
        let property_object = properties
            .as_object()
            .ok_or_else(|| "record properties must be an object".to_string())?;
        let props = property_object
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    rushdb_lmpg::import::coerce_change(value, &options)
                        .unwrap_or_else(|| rushdb_lmpg::plan::FieldChange::Set(value.clone())),
                )
            })
            .collect();

        let draft_id = *uuid::Uuid::now_v7().as_bytes();
        let mut plan = BfsPlan::default();
        plan.records.push(BfsRecord {
            draft_id,
            label: label.to_string(),
            properties: props,
            explicit_id: None,
        });
        if !vectors.is_empty() {
            let entries: Vec<InlineVectorEntry> =
                serde_json::from_value(serde_json::Value::Array(vectors.to_vec()))
                    .map_err(|error| format!("invalid inline vector: {error}"))?;
            if !entries.is_empty() {
                plan.vector_drafts.push(rushdb_lmpg::import::VectorDraft {
                    draft_id,
                    label: label.to_string(),
                    entries,
                });
            }
        }

        self.validate_inline_vectors(&plan)?;
        let idmap = import_bfs(&self.lmpg(), &plan, &options).map_err(|e| e.to_reason())?;
        self.bind_inline_vectors(&plan, &idmap)?;
        let persisted = idmap
            .iter()
            .find(|m| m.draft_id == draft_id)
            .map_or(draft_id, |m| m.persisted_id);
        self.record_by_id_hex(&persisted)
    }

    /// Create a record while enforcing the caller-owned execution deadline.
    /// Creation has no existing-record lookup, so the pinned view is used for
    /// admission/deadline ownership rather than target selection.
    pub(crate) fn create_record_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        id: Option<&str>,
        label: &str,
        properties: &serde_json::Value,
        vectors: &[serde_json::Value],
        options: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        use rushdb_lmpg::import::{
            import_bfs_with_cancel, BfsPlan, BfsRecord, ImportOptions, InlineVectorEntry,
        };
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        ImportOptions::validate_json(options)?;
        let options = ImportOptions::from_json(options);
        let property_object = properties
            .as_object()
            .ok_or_else(|| "record properties must be an object".to_string())?;
        let props = property_object
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    rushdb_lmpg::import::coerce_change(value, &options)
                        .unwrap_or_else(|| rushdb_lmpg::plan::FieldChange::Set(value.clone())),
                )
            })
            .collect();
        // A supplied id is honoured; creating over an existing id
        // fails, as `create` promises.
        let explicit_id = id.map(bytes_from_hex).transpose()?;
        if let Some(explicit) = explicit_id {
            if rushdb_lmpg::reader::try_access::record_by_id(
                context.read_view(),
                self.tenant(),
                &RecordId(explicit),
            )
            .map_err(|e| e.to_string())?
            .is_some()
            {
                return Err(format!("record {} already exists", id.unwrap_or_default()));
            }
        }
        let draft_id = explicit_id.unwrap_or_else(|| *uuid::Uuid::now_v7().as_bytes());
        let mut plan = BfsPlan::default();
        plan.records.push(BfsRecord {
            draft_id,
            label: label.to_string(),
            properties: props,
            explicit_id,
        });
        if !vectors.is_empty() {
            let entries: Vec<InlineVectorEntry> =
                serde_json::from_value(serde_json::Value::Array(vectors.to_vec()))
                    .map_err(|error| format!("invalid inline vector: {error}"))?;
            if !entries.is_empty() {
                plan.vector_drafts.push(rushdb_lmpg::import::VectorDraft {
                    draft_id,
                    label: label.to_string(),
                    entries,
                });
            }
        }
        self.validate_inline_vectors(&plan)?;
        let deadline = context.deadline();
        let idmap = import_bfs_with_cancel(&self.lmpg(), &plan, &options, &|| {
            deadline.is_some_and(|at| std::time::Instant::now() >= at)
        })
        .map_err(|e| e.to_reason())?;
        if let Err(bind_error) = self.bind_inline_vectors_with_cancel(&plan, &idmap, &|| {
            deadline.is_some_and(|at| std::time::Instant::now() >= at)
        }) {
            if options.merge_by.is_none() && options.merge_strategy.is_none() {
                if let Err(rollback_error) = self.rollback_ingest_records(&idmap) {
                    return Err(format!(
                        "vector binding failed after ingest ({bind_error}); rollback also failed ({rollback_error})"
                    ));
                }
                return Err(format!(
                    "vector binding failed; ingest rolled back: {bind_error}"
                ));
            }
            return Err(format!(
                "vector binding failed after partial ingest; retry is idempotent: {bind_error}"
            ));
        }
        let persisted = idmap
            .iter()
            .find(|m| m.draft_id == draft_id)
            .map_or(draft_id, |m| m.persisted_id);
        let record = self.record_by_id_hex(&persisted)?;
        Ok(record)
    }

    /// Upsert without a label (`create` with `mergeBy`/`mergeStrategy` and no
    /// `label`): update the record of any label whose merge keys match, as
    /// `merge` (or `replace` under `mergeStrategy: "rewrite"`) would. Without
    /// a label nothing is created.
    pub(crate) fn upsert_unlabeled_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        properties: &serde_json::Value,
        vectors: &[serde_json::Value],
        options: &serde_json::Value,
    ) -> Result<UnlabeledUpsert, String> {
        use rushdb_lmpg::import::ImportOptions;

        ImportOptions::validate_json(options)?;
        let options = ImportOptions::from_json(options);
        let props: Vec<(String, rushdb_lmpg::plan::FieldChange)> = properties
            .as_object()
            .ok_or_else(|| "record properties must be an object".to_string())?
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    rushdb_lmpg::import::coerce_change(value, &options)
                        .unwrap_or_else(|| rushdb_lmpg::plan::FieldChange::Set(value.clone())),
                )
            })
            .collect();
        let mut matches =
            rushdb_lmpg::import::find_by_keys_any_label(&self.lmpg(), &props, &options)
                .map_err(|e| e.to_reason())?;
        if matches.len() > 1 {
            return Ok(UnlabeledUpsert::Ambiguous(
                matches.into_iter().map(|(label, _)| label).collect(),
            ));
        }
        let Some((_, record)) = matches.pop() else {
            return Ok(UnlabeledUpsert::NoMatch);
        };
        let id = uuid::Uuid::from_bytes(record.record_id)
            .simple()
            .to_string();
        if options.merge_strategy.as_deref() == Some("rewrite") {
            self.replace_record_with_execution_context(context, &id, properties, vectors)
        } else {
            self.merge_record_with_execution_context(context, &id, None, properties, vectors)
        }
        .map(UnlabeledUpsert::Updated)
    }

    /// Whether a record with this id exists at the request's snapshot.
    pub(crate) fn record_exists(
        &self,
        context: &rushdb_engine::ExecutionContext,
        id_hex: &str,
    ) -> Result<bool, String> {
        let id = RecordId(bytes_from_hex(id_hex)?);
        Ok(
            rushdb_lmpg::reader::try_access::record_by_id(context.read_view(), self.tenant(), &id)
                .map_err(|e| e.to_string())?
                .is_some(),
        )
    }

    /// Fetch one canonical record by compact hex id.
    ///
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn get_record(&self, id_hex: &str) -> Result<serde_json::Value, String> {
        let id = RecordId(bytes_from_hex(id_hex)?);
        let view = self.store.read_view().map_err(|e| e.to_string())?;
        self.get_record_in_view(&view, id)
    }

    /// Fetch one record using the caller's pinned execution view.
    pub(crate) fn get_record_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        id_hex: &str,
    ) -> Result<serde_json::Value, String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let id = RecordId(bytes_from_hex(id_hex)?);
        let value = self.get_record_in_view(context.read_view(), id)?;
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        Ok(value)
    }

    pub(super) fn get_record_in_view(
        &self,
        view: &rushdb_storage::ReadView,
        id: RecordId,
    ) -> Result<serde_json::Value, String> {
        let hdr = rushdb_lmpg::reader::try_access::record_by_id(view, self.tenant(), &id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "record not found".to_string())?;
        self.record_value_in_view(view, hdr.ordinal)
    }

    /// PATCH semantics: merge incoming properties onto the existing record.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn merge_record(
        &self,
        id_hex: &str,
        properties: &serde_json::Value,
        vectors: &[serde_json::Value],
    ) -> Result<serde_json::Value, String> {
        use rushdb_lmpg::import::{BfsPlan, InlineVectorEntry, UpsertIdMap, VectorDraft};

        let id_bytes = bytes_from_hex(id_hex)?;
        let id = RecordId(id_bytes);
        let lmpg = self.lmpg();
        let snap = self.store.read_view().map_err(|e| e.to_string())?;
        let hdr = rushdb_lmpg::reader::try_access::record_by_id(&snap, self.tenant(), &id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "record not found".to_string())?;
        let props = Self::props_from_properties(properties)?;
        let rr = rushdb_lmpg::RecordRef {
            record_id: hdr.record_id,
            ordinal: hdr.ordinal,
        };
        let mut vector_plan = BfsPlan::default();
        if !vectors.is_empty() {
            let entries: Vec<InlineVectorEntry> =
                serde_json::from_value(serde_json::Value::Array(vectors.to_vec()))
                    .map_err(|error| format!("invalid inline vectors: {error}"))?;
            if !entries.is_empty() {
                vector_plan.vector_drafts.push(VectorDraft {
                    draft_id: id_bytes,
                    label: hdr.label.clone(),
                    entries,
                });
            }
        }
        self.validate_inline_vectors(&vector_plan)?;
        lmpg.update_fields(rr, props).map_err(|e| e.to_reason())?;
        if let Err(error) = self.bind_inline_vectors(
            &vector_plan,
            &[UpsertIdMap {
                draft_id: id_bytes,
                persisted_id: id_bytes,
            }],
        ) {
            return Err(format!(
                "vector binding failed after partial merge; retry is idempotent: {error}"
            ));
        }
        self.record_value(hdr.ordinal)
    }

    /// PATCH semantics using the caller-owned request snapshot for target
    /// lookup and reconstruction inputs.
    /// Merge updates the record when `id_hex` exists and otherwise creates
    /// it with that id under `label`, as `PutMode::Merge` promises.
    pub(crate) fn merge_record_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        id_hex: &str,
        label: Option<&str>,
        properties: &serde_json::Value,
        vectors: &[serde_json::Value],
    ) -> Result<serde_json::Value, String> {
        use rushdb_lmpg::import::{BfsPlan, InlineVectorEntry, UpsertIdMap, VectorDraft};

        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let id_bytes = bytes_from_hex(id_hex)?;
        let id = RecordId(id_bytes);
        let lmpg = self.lmpg();
        let Some(hdr) =
            rushdb_lmpg::reader::try_access::record_by_id(context.read_view(), self.tenant(), &id)
                .map_err(|e| e.to_string())?
        else {
            let label = label.ok_or_else(|| {
                "record not found; merge creates a missing record only when `label` is given"
                    .to_string()
            })?;
            return self.create_record_with_execution_context(
                context,
                Some(id_hex),
                label,
                properties,
                vectors,
                &serde_json::json!({}),
            );
        };
        let props = Self::props_from_properties(properties)?;
        let rr = rushdb_lmpg::RecordRef {
            record_id: hdr.record_id,
            ordinal: hdr.ordinal,
        };
        let mut vector_plan = BfsPlan::default();
        if !vectors.is_empty() {
            let entries: Vec<InlineVectorEntry> =
                serde_json::from_value(serde_json::Value::Array(vectors.to_vec()))
                    .map_err(|error| format!("invalid inline vectors: {error}"))?;
            if !entries.is_empty() {
                vector_plan.vector_drafts.push(VectorDraft {
                    draft_id: id_bytes,
                    label: hdr.label.clone(),
                    entries,
                });
            }
        }
        self.validate_inline_vectors(&vector_plan)?;
        lmpg.update_fields(rr, props).map_err(|e| e.to_reason())?;
        if let Err(error) = self.bind_inline_vectors(
            &vector_plan,
            &[UpsertIdMap {
                draft_id: id_bytes,
                persisted_id: id_bytes,
            }],
        ) {
            return Err(format!(
                "vector binding failed after partial merge; retry is idempotent: {error}"
            ));
        }
        self.record_value(hdr.ordinal)
    }

    /// PUT semantics: replace all properties on the existing record.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn replace_record(
        &self,
        id_hex: &str,
        properties: &serde_json::Value,
        vectors: &[serde_json::Value],
    ) -> Result<serde_json::Value, String> {
        use rushdb_lmpg::import::{BfsPlan, InlineVectorEntry, UpsertIdMap, VectorDraft};

        let id_bytes = bytes_from_hex(id_hex)?;
        let id = RecordId(id_bytes);
        let lmpg = self.lmpg();
        let snap = self.store.read_view().map_err(|e| e.to_string())?;
        let hdr = rushdb_lmpg::reader::try_access::record_by_id(&snap, self.tenant(), &id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "record not found".to_string())?;
        let rr = rushdb_lmpg::RecordRef {
            record_id: hdr.record_id,
            ordinal: hdr.ordinal,
        };

        let mut vector_plan = BfsPlan::default();
        if !vectors.is_empty() {
            let entries: Vec<InlineVectorEntry> =
                serde_json::from_value(serde_json::Value::Array(vectors.to_vec()))
                    .map_err(|error| format!("invalid inline vectors: {error}"))?;
            if !entries.is_empty() {
                vector_plan.vector_drafts.push(VectorDraft {
                    draft_id: id_bytes,
                    label: hdr.label.clone(),
                    entries,
                });
            }
        }
        self.validate_inline_vectors(&vector_plan)?;

        // remove current properties then write the provided set
        let existing: Vec<String> =
            rushdb_lmpg::reconstruct::reconstruct_record(&snap, self.tenant(), hdr.ordinal)
                .map_err(|e| e.to_string())?
                .and_then(|r| r.as_object().cloned())
                .map(|o| {
                    o.keys()
                        .filter(|k| !rushdb_types::value::is_reserved_name(k))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
        // One commit: the new properties, plus a delete of every existing
        // one they do not name. A refused commit leaves the record as it was.
        let mut fields = Self::props_from_properties(properties)?;
        let named: std::collections::HashSet<String> =
            fields.iter().map(|(name, _)| name.clone()).collect();
        fields.extend(
            existing
                .into_iter()
                .filter(|name| !named.contains(name))
                .map(|name| (name, rushdb_lmpg::plan::FieldChange::Delete)),
        );
        lmpg.update_fields(rr, fields).map_err(|e| e.to_reason())?;
        if let Err(error) = self.bind_inline_vectors(
            &vector_plan,
            &[UpsertIdMap {
                draft_id: id_bytes,
                persisted_id: id_bytes,
            }],
        ) {
            return Err(format!(
                "vector binding failed after partial replacement; retry is idempotent: {error}"
            ));
        }
        self.record_value(hdr.ordinal)
    }

    /// PUT semantics using the caller-owned request snapshot for target and
    /// existing-property discovery.
    pub(crate) fn replace_record_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        id_hex: &str,
        properties: &serde_json::Value,
        vectors: &[serde_json::Value],
    ) -> Result<serde_json::Value, String> {
        use rushdb_lmpg::import::{BfsPlan, InlineVectorEntry, UpsertIdMap, VectorDraft};

        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let id_bytes = bytes_from_hex(id_hex)?;
        let id = RecordId(id_bytes);
        let lmpg = self.lmpg();
        let snap = context.read_view();
        let hdr = rushdb_lmpg::reader::try_access::record_by_id(snap, self.tenant(), &id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "record not found".to_string())?;
        let rr = rushdb_lmpg::RecordRef {
            record_id: hdr.record_id,
            ordinal: hdr.ordinal,
        };
        let mut vector_plan = BfsPlan::default();
        if !vectors.is_empty() {
            let entries: Vec<InlineVectorEntry> =
                serde_json::from_value(serde_json::Value::Array(vectors.to_vec()))
                    .map_err(|error| format!("invalid inline vectors: {error}"))?;
            if !entries.is_empty() {
                vector_plan.vector_drafts.push(VectorDraft {
                    draft_id: id_bytes,
                    label: hdr.label.clone(),
                    entries,
                });
            }
        }
        self.validate_inline_vectors(&vector_plan)?;
        let existing: Vec<String> =
            rushdb_lmpg::reconstruct::reconstruct_record(snap, self.tenant(), hdr.ordinal)
                .map_err(|e| e.to_string())?
                .and_then(|r| r.as_object().cloned())
                .map(|o| {
                    o.keys()
                        .filter(|k| !rushdb_types::value::is_reserved_name(k))
                        .cloned()
                        .collect()
                })

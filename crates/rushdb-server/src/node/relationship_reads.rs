//! Node relationship reads: the paged relationship listing (by endpoint
//! predicates or a streamed scan) and the relationship schema description.

use super::{
    access, schema_json_type, type_name_str, Arc, BTreeMap, OwnerKind, RelationshipAggregate,
    RequestScope,
};

impl RequestScope<'_> {
    /// Scan relationships with endpoint, type, owned-property, and pagination
    /// predicates.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn scan_relationships(
        &self,
        query: &rushdb_protocol::RelationshipQuery,
    ) -> Result<serde_json::Value, String> {
        let snap = Arc::new(self.store.read_view().map_err(|e| e.to_string())?);
        self.scan_relationships_in_view(query, &snap)
    }

    /// Scan relationships using a caller-owned pinned execution view.
    pub(crate) fn scan_relationships_with_execution_context(
        &self,
        context: &mut rushdb_engine::ExecutionContext,
        query: &rushdb_protocol::RelationshipQuery,
    ) -> Result<serde_json::Value, String> {
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        let result = self.scan_relationships_in_view(query, context.read_view())?;
        context
            .check_deadline()
            .map_err(|error| error.to_string())?;
        Ok(result)
    }

    pub(super) fn scan_relationships_in_view(
        &self,
        query: &rushdb_protocol::RelationshipQuery,
        snap: &Arc<rushdb_storage::ReadView>,
    ) -> Result<serde_json::Value, String> {
        let type_filter = query.relation_type.as_ref();
        let predicate = match &query.predicate {
            serde_json::Value::Null => None,
            serde_json::Value::Object(map) if map.is_empty() => None,
            where_ => {
                Some(rushdb_query::parse_property_predicate(where_).map_err(|e| e.to_string())?)
            }
        };
        let limit = query.limit.unwrap_or(10_000);
        let skip = query.offset;

        let tenant = self.tenant();
        let active_policy = rushdb_authz::load_active_policy_in_view(snap, tenant)
            .map_err(|error| error.to_string())?;
        let authz = rushdb_lmpg::AuthorizedView::new(snap, tenant, active_policy.as_ref());

        // One relationship against every filter: its row when it matches,
        // reconstructed only when the predicate needs it or it is on the page.
        let mut total = 0u64;
        let mut rows = Vec::new();
        let mut consider =
            |hdr: rushdb_types::occurrence::RelationshipHeader| -> Result<(), String> {
                if type_filter.is_some_and(|t| hdr.rel_type != *t)
                    || !authz.rel_type_allowed(&hdr.rel_type)
                {
                    return Ok(());
                }
                let src_hdr = access::record_by_ord(snap, tenant, hdr.source_ord)
                    .map_err(|e| e.to_string())?
                    .ok_or("corrupt relationship: missing source")?;
                let tgt_hdr = access::record_by_ord(snap, tenant, hdr.target_ord)
                    .map_err(|e| e.to_string())?
                    .ok_or("corrupt relationship: missing target")?;
                let label_ok = |wanted: &[String], label: &String| {
                    wanted.is_empty() || wanted.iter().any(|l| l == label)
                };
                if !label_ok(&query.source_labels, &src_hdr.label)
                    || !label_ok(&query.target_labels, &tgt_hdr.label)
                    || !authz.label_allowed(&src_hdr.label)
                    || !authz.label_allowed(&tgt_hdr.label)
                {
                    return Ok(());
                }
                let on_page = total >= skip && (rows.len() as u64) < limit;
                if predicate.is_none() && !on_page {
                    total += 1;
                    return Ok(());
                }
                let mut rel_obj =
                    rushdb_lmpg::reconstruct::reconstruct_relationship(snap, tenant, hdr.ordinal)
                        .map_err(|e| e.to_string())?
                        .and_then(|v| v.as_object().cloned())
                        .ok_or("corrupt relationship: reconstruction missing")?;
                if let Some(predicate) = &predicate {
                    if !rushdb_query::matches_properties(predicate, &rel_obj)
                        .map_err(|e| e.to_string())?
                    {
                        return Ok(());
                    }
                }
                total += 1;
                if !on_page {
                    return Ok(());
                }
                rel_obj.retain(|name, _| !rushdb_types::value::is_reserved_name(name));
                rows.push(serde_json::json!({
                    "id": uuid::Uuid::from_bytes(hdr.relationship_id).to_string(),
                    "type": hdr.rel_type,
                    "sourceId": uuid::Uuid::from_bytes(hdr.source_id).to_string(),
                    "targetId": uuid::Uuid::from_bytes(hdr.target_id).to_string(),
                    "sourceLabel": src_hdr.label,
                    "targetLabel": tgt_hdr.label,
                    "properties": rel_obj,
                }));
                Ok(())
            };

        if let Some(headers) = self.endpoint_relationships(query, snap)? {
            // Endpoint filters: only the matching records' own links.
            for hdr in headers {
                consider(hdr)?;
            }
        } else {
            for_each_relationship(snap, tenant, &mut consider)?;
        }
        // `total` is the number of relationships matching the filters before
        // pagination (the page is `data`).
        Ok(serde_json::json!({ "data": rows, "total": total }))
    }

    /// The relationships whose source and/or target satisfy the query's
    /// endpoint predicates, found through those records' adjacency, in
    /// creation order; `None` when the query has no endpoint predicate.
    fn endpoint_relationships(
        &self,
        query: &rushdb_protocol::RelationshipQuery,
        snap: &Arc<rushdb_storage::ReadView>,

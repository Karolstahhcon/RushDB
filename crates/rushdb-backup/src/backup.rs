//! Logical JSONL backup and restore.
//!
//! This is the retained half of the former `rushdb-migrate` crate: export a
//! tenant's records and relationships as newline-delimited JSON, and restore
//! them into a live engine preserving public identity. It is **not** a
//! canonical storage checkpoint (policy/index metadata, outcomes, history, and
//! journal state are not included), and it is **not** a migration tool —
//! migrations are performed outside the engine.

use std::io::BufRead;

use rushdb_lmpg::Lmpg;

/// Export one tenant's records and relationships as a JSONL document, returning
/// the text plus `(records, relationships)` counts.
///
/// # Errors
///
/// Returns an error string if the underlying export fails or the produced bytes
/// are not valid UTF-8.
pub fn export_tenant_jsonl(
    snap: &rushdb_storage::ReadView,
    tenant: rushdb_types::ids::TenantId,
) -> Result<(String, u64, u64), String> {
    let mut bytes = Vec::new();
    let (records, relationships) = crate::export::export_tenant_jsonl_to(snap, tenant, &mut bytes)?;
    let jsonl = String::from_utf8(bytes).map_err(|e| e.to_string())?;
    Ok((jsonl, records, relationships))
}

/// Restore one record line: upsert by `__id` and re-apply the user-facing
/// `__id` property that `upsert_document` consumes as physical identity.
fn restore_record(lmpg: &Lmpg, v: &serde_json::Value) -> Result<(), String> {
    let label = v
        .get("label")
        .and_then(|x| x.as_str())

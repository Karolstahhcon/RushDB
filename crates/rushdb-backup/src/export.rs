//! Pinned logical graph export. This is not a canonical storage checkpoint:
//! policy/index metadata, outcomes, history, and journal state are not included.

use std::io::Write;

use rushdb_lmpg::{reader::try_access, reconstruct};
use rushdb_storage::{ReadView, ScanBounds, ScanProjection};
use rushdb_types::{
    ids::TenantId,
    keys::{key, P_REL},
    occurrence::{RecordHeader, RelationshipHeader},
};

/// Write records first, then relationships, from exactly one retained view.
/// Header scans are bounded to 128 rows / 1 MiB per batch. Reconstruction is
/// still eager per owner and subject to the view's local read limits; there
/// is not yet a shared graph/operator memory budget.
///
/// Errors stop export and are never represented as missing rows. A writer can
/// contain an incomplete prefix after failure; callers must discard it rather
/// than publish it as a valid backup. Flush/fsync/atomic file publication are
/// the caller's responsibility, not implied by a successful return here.
///
/// # Errors
///
/// Returns an error string if a header row is malformed, a key/header or
/// ordinal mapping is inconsistent, a record/relationship cannot be
/// reconstructed, the view is cancelled, the writer rejects bytes, or the
/// exported count overflows.
pub fn export_tenant_jsonl_to<W: Write + ?Sized>(
    view: &ReadView,
    tenant: TenantId,
    writer: &mut W,
) -> Result<(u64, u64), String> {
    let mut counts = [0u64; 2];
    // P20: one reconstruction session per export shares the family-definition
    // cache across all owners (families are frozen within this pinned view).
    let mut session = reconstruct::ReconstructSession::new(view, tenant);
    for (kind, count) in counts.iter_mut().enumerate() {
        // Records: every header row (`RECORD_HDR`). Relationships: the id-keyed
        // header rows (a trailing separator excludes relationship-ordinal).
        let prefix = if kind == 0 {
            rushdb_types::keys::record_hdr_prefix(tenant)
        } else {
            key(tenant, &[P_REL, b""])
        };
        let mut cursor = view
            .scan(ScanBounds::prefix(&prefix), ScanProjection::KeyValue)
            .map_err(|e| e.to_string())?;
        loop {
            let batch = cursor
                .next_batch(128, 1024 * 1024)
                .map_err(|e| e.to_string())?;
            for row in batch.rows {
                let raw = row.value.ok_or("export: missing header value")?;
                let line = if kind == 0 {
                    record_line(&mut session, view, tenant, &row.key, &raw)?
                } else {
                    relationship_line(&mut session, view, tenant, &row.key, &raw)?
                };
                view.check_cancelled().map_err(|e| e.to_string())?;
                serde_json::to_writer(&mut *writer, &line).map_err(|e| e.to_string())?;
                writer.write_all(b"\n").map_err(|e| e.to_string())?;

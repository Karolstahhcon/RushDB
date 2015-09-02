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
                view.check_cancelled().map_err(|e| e.to_string())?;
                *count = count.checked_add(1).ok_or("export count overflow")?;
            }
            if batch.done {
                break;
            }
        }
    }
    // Every header was resolved through its identity row above; a surplus
    // identity row (an id pointing at another record's ordinal) would
    // otherwise be silently dropped from the export.
    let identities = count_keys(view, &rushdb_types::keys::record_id_prefix(tenant))?;
    if identities != counts[0] {
        return Err("export: record identity index does not match headers".into());
    }
    Ok((counts[0], counts[1]))
}

/// Number of keys under `prefix`, scanned in bounded key-only batches.
fn count_keys(view: &ReadView, prefix: &[u8]) -> Result<u64, String> {
    let mut cursor = view
        .scan(ScanBounds::prefix(prefix), ScanProjection::Keys)
        .map_err(|e| e.to_string())?;
    let mut total = 0u64;
    loop {
        let batch = cursor
            .next_batch(1024, 1024 * 1024)
            .map_err(|e| e.to_string())?;
        total = total
            .checked_add(batch.rows.len() as u64)
            .ok_or("export count overflow")?;
        if batch.done {
            return Ok(total);
        }
    }
}

/// One exported record line from its header row (`ordinal` key suffix).
fn record_line(
    session: &mut reconstruct::ReconstructSession<'_, ReadView>,
    view: &ReadView,
    tenant: TenantId,
    header_key: &[u8],
    raw: &[u8],
) -> Result<serde_json::Value, String> {
    let ordinal = rushdb_types::keys::parse_record_hdr_key(header_key)
        .ok_or("export: invalid record header key")?;
    let (record_id, label) =
        rushdb_types::keys::decode_record_hdr(raw).ok_or("export: invalid record header")?;
    let hdr = RecordHeader {
        record_id,
        ordinal,
        label: label.to_owned(),
    };
    let indexed = try_access::record_by_id(view, tenant, &rushdb_types::ids::RecordId(record_id))
        .map_err(|e| e.to_string())?
        .ok_or("export: missing record id mapping")?;
    if indexed.ordinal != hdr.ordinal {
        return Err("export: record id/ordinal identity mismatch".into());
    }
    let rec = session
        .reconstruct_record(hdr.ordinal)
        .map_err(|e| e.to_string())?

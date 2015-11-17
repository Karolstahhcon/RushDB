//! Budgeted page reconstruction for the structured query read path.

use super::{
    attach_collects_with_budget, reconstruct, Lmpg, QueryError, QuerySnapshot, SearchRequest,
};

/// Resolve the effective working-set budget: the caller's remaining
/// request budget, else the configured per-query cap.
///
/// `Some(0)` from a caller is a *hard zero cap* (an exhausted request budget),
/// distinct from a configured `query_memory_max_bytes == 0`, which disables
/// the cap. Conflating the two let an exhausted request run with no limit.
#[must_use]
pub fn effective_budget(work_budget: Option<usize>, config: u64) -> Option<u64> {
    match work_budget {
        Some(bytes) => Some(u64::try_from(bytes).unwrap_or(u64::MAX)),
        None => (config != 0).then_some(config),
    }
}

/// Charge one record's serialized size against `budget`, accumulating into
/// `charged`. Errors once the running total exceeds the limit.
///
/// `reservation`, when given, is grown to the real running total:
/// best-effort attribution against the shared graph-phase budget, not a new
/// rejection path — only `budget` above can fail this call, so a tight
/// shared pool just means the gauge under-reports.
pub(super) fn charge_record(
    budget: Option<u64>,
    charged: &mut u64,
    reservation: Option<&mut rushdb_storage::resources::ByteReservation>,
    record: &serde_json::Value,
) -> Result<(), QueryError> {
    if let Some(limit) = budget {
        let bytes =
            serde_json::to_vec(record).map_or(0, |v| u64::try_from(v.len()).unwrap_or(u64::MAX));
        *charged = charged.saturating_add(bytes);
        if let Some(reservation) = reservation {
            let _ = reservation.resize(usize::try_from(*charged).unwrap_or(usize::MAX));
        }
        if *charged > limit {
            return Err(QueryError::ResourceLimit(format!(
                "query working set exceeded {limit} bytes"
            )));
        }
    }
    Ok(())
}


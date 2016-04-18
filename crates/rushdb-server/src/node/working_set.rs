//! Memory a catalog read holds while it works (value dictionaries and the
//! values matched from them), capped like a query's working set by
//! `query-memory-max`, tightened by the request (READ-1, QRY-3). Charges
//! also poll cancellation and the request deadline, so a long dictionary
//! read stops when the request does.

use std::time::Instant;

use super::RequestScope;

/// How many charges pass between cancellation and deadline checks.
const CHECK_EVERY: u32 = 1024;

/// The bytes one distinct value holds in a working map: its encoded form
/// twice (key and stored value) plus the slot, as the facet scan counts it.
pub(crate) fn value_bytes(encoded_len: usize) -> u64 {
    u64::try_from(encoded_len)
        .unwrap_or(u64::MAX)
        .saturating_mul(2)
        .saturating_add(64)
}

/// A request's working-set meter for catalog reads.
pub(crate) struct WorkingSet<'a> {
    limit: Option<u64>,
    charged: u64,
    is_cancelled: &'a dyn Fn() -> bool,
    deadline: Option<Instant>,
    limits: Option<rushdb_lmpg::QueryLimits>,
    charges: u32,
}

impl RequestScope<'_> {
    /// A meter under this request's bounds: its working-set cap and time,
    /// and `deadline` (the execution context's), stopping when
    /// `is_cancelled` says so.
    pub(crate) fn working_set<'a>(
        &self,
        is_cancelled: &'a dyn Fn() -> bool,
        deadline: Option<Instant>,
    ) -> WorkingSet<'a> {
        let limits = self.lmpg().limits();
        let mut meter = WorkingSet::new(limits.query_memory_max_bytes(), is_cancelled, deadline);
        meter.limits = Some(limits);
        meter

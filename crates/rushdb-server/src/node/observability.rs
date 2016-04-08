//! Node observability operations. Context entry points retain caller-owned
//! snapshot, tenant, cancellation, and budget semantics.

use rushdb_types::ids::TenantId;

use super::Node;

impl Node {
    // ---- hybrid ----------------------------------------------------------

    // ---- operations surface ------------------------------------------

    /// Request counters snapshot consumed by the metrics endpoint.
    /// Record one request observation (route + latency) into the histogram.
    pub fn observe(&self, route: &str, seconds: f64) {
        self.metrics.observe(route, seconds);
    }

    /// Record rows returned by a query route (pure data-queried metric).
    pub fn observe_rows(&self, route: &str, rows: u64) {
        self.metrics.observe_rows(route, rows);
    }

    /// Record one maintenance-tick step's duration (`step` is a stable
    /// label: `read_pins`, `vector_segments`, `housekeeping`, `history_gc`,
    /// `retention`, `receipts`, `vector_slots`, `checkpoint`).
    /// Returns the next step's start.
    pub(crate) fn observe_maintenance_step(
        &self,
        step: &str,
        started: std::time::Instant,
    ) -> std::time::Instant {
        let now = std::time::Instant::now();
        self.metrics
            .observe_maintenance_step(step, (now - started).as_secs_f64());
        now
    }

    /// Record one maintenance tick's duration (histogram).
    pub(crate) fn observe_gc_tick(&self, seconds: f64) {
        self.metrics.observe_gc_tick(seconds);
    }

    /// Record the HTTP request-body bytes currently reserved in flight.
    pub(crate) fn observe_inflight_body_bytes(&self, bytes: usize) {
        self.metrics.observe_inflight_body_bytes(bytes);
    }

    /// Record bounded work performed by an operation (for example candidate
    /// rows, reconstructed rows, payload gets, or posting bytes).
    pub fn observe_work(&self, operation: &str, counter: &str, units: u64) {
        self.metrics.observe_work(operation, counter, units);
    }

    /// Record one boot-phase duration for startup attribution.
    pub fn observe_boot_phase(&self, phase: &str, seconds: f64) {

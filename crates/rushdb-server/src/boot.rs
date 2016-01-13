//! Boot-time listener: the HTTP port answers from process start (C6,
//! cloud-capacity F6).
//!
//! Opening a large store means redb open or repair, WAL replay and the
//! derived-index rebuild, which take tens of seconds. Before this module the
//! listener was bound only afterwards, so an orchestrator probing `/health`
//! saw "connection refused" and could restart the process in a loop.
//! [`BootListener`] binds the configured HTTP address first and answers on it
//! until the node is ready:
//! - `/health` → 200 `{"status":"starting","phase":…}` (the process is alive);
//! - `/ready` → 503 with the current [`phase`];
//! - anything else → 503 with the protocol `unavailable` error (retryable)
//!   and `Retry-After: 1`.
//!
//! [`BootListener::finish`] stops answering and hands the **same** socket to
//! the real router, so there is no window in which the port is closed.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Ordered boot phases reported by `/ready` while the node starts.
pub const PHASES: [&str; 4] = ["starting", "opening-storage", "starting-engines", "ready"];

static PHASE: AtomicUsize = AtomicUsize::new(0);

/// Record the current boot phase (an index into [`PHASES`]).
pub fn set_phase(phase: &str) {
    if let Some(index) = PHASES.iter().position(|known| *known == phase) {
        PHASE.store(index, Ordering::Release);
    }
}

/// The current boot phase name.
#[must_use]
pub fn phase() -> &'static str {
    PHASES[PHASE.load(Ordering::Acquire).min(PHASES.len() - 1)]
}

/// A pre-bound HTTP listener that answers health/readiness probes while the
/// node boots.
pub struct BootListener {
    stop: Arc<AtomicBool>,
    worker: std::thread::JoinHandle<TcpListener>,
}

impl BootListener {
    /// Bind `addr:port` and start answering probes.
    ///
    /// # Errors
    /// Returns a message when the address cannot be bound.
    pub fn start(addr: &str, port: u16) -> Result<Self, String> {
        let listener = TcpListener::bind((addr, port)).map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let stop = Arc::new(AtomicBool::new(false));
        let started = Instant::now();
        let worker = {

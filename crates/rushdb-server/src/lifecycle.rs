//! Ownership of transport threads and their bounded shutdown. A timed-out
//! thread is a failed shutdown; the binary exits unsuccessfully rather than
//! claiming the database closed cleanly.

use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Owns all auxiliary transport threads under one shutdown deadline.
pub struct TransportLifecycle {
    shutdown: Box<dyn Fn()>,
    threads: Vec<(&'static str, JoinHandle<Result<(), String>>)>,
    timeout: Duration,
}

impl TransportLifecycle {
    /// Register a shutdown signal and the total time allowed for draining.
    pub fn new(shutdown: impl Fn() + 'static, timeout: Duration) -> Self {
        Self {
            shutdown: Box::new(shutdown),
            threads: Vec::new(),
            timeout,
        }
    }

    /// Start and retain a named transport worker.
    pub fn spawn(
        &mut self,
        name: &'static str,
        serve: impl FnOnce() -> Result<(), String> + Send + 'static,
    ) {
        self.threads.push((name, std::thread::spawn(serve)));
    }

    /// Signal all transports and wait under one deadline.
    ///
    /// # Errors
    /// Returns names of workers that failed, panicked or exceeded the deadline.
    pub fn shutdown(&mut self) -> Result<(), Vec<&'static str>> {
        (self.shutdown)();
        let deadline = Instant::now() + self.timeout;
        let mut failures = Vec::new();
        for (name, worker) in self.threads.drain(..) {
            while !worker.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            if !worker.is_finished() {
                tracing::error!(name, "transport join deadline elapsed");
                failures.push(name);
                continue;
            }
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::error!(name, %error, "transport failed");
                    failures.push(name);
                }
                Err(_) => {
                    tracing::error!(name, "transport panicked");
                    failures.push(name);
                }
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }
}

impl Drop for TransportLifecycle {
    fn drop(&mut self) {
        if let Err(failures) = self.shutdown() {
            tracing::error!(?failures, "transport shutdown incomplete");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    };

    #[test]
    fn shutdown_signals_and_joins_owned_workers() {
        let stop = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&stop);
        let mut owner = TransportLifecycle::new(
            move || {
                signal.store(true, Ordering::Release);
            },
            Duration::from_secs(1),
        );
        owner.spawn("cooperative", move || {
            while !stop.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            Ok(())
        });
        assert!(owner.shutdown().is_ok());
        assert!(owner.threads.is_empty());
    }

    #[test]
    fn blocked_worker_reports_failure_without_unbounded_join() {
        let (release, blocked) = mpsc::channel();
        let (done, finished) = mpsc::channel();
        let mut owner = TransportLifecycle::new(|| {}, Duration::ZERO);
        owner.spawn("blocked", move || {
            let _ = blocked.recv();
            let _ = done.send(());
            Ok(())
        });
        assert_eq!(owner.shutdown(), Err(vec!["blocked"]));
        release.send(()).unwrap();
        finished.recv_timeout(Duration::from_secs(1)).unwrap();
    }
}

/// Tracks maintenance closures running on Tokio's blocking pool.
///
/// Aborting a maintenance task's async loop does **not** stop a closure
/// already running in `spawn_blocking`. Before this tracker, shutdown aborted
/// the loops, checkpointed while a keyspace scan still ran, and left the
/// runtime with that closure still holding the `Store`. The redb `Database` was
/// never dropped, and every restart paid a full repair (C6, cloud-capacity
/// F5). Every maintenance closure is spawned through [`Self::spawn`], and
/// shutdown waits for [`Self::drained`] before the final checkpoint.
#[derive(Clone, Debug, Default)]
pub struct MaintenanceWork {
    inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

struct InflightGuard(std::sync::Arc<std::sync::atomic::AtomicUsize>);

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

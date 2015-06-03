//! Bounded blocking work scheduler for embedded engine callers.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use rushdb_types::ids::TenantId;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Scheduler admission failures are explicit instead of silently growing an
/// unbounded blocking-work queue.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SchedulerError {
    /// The bounded queue has no free slot.
    #[error("engine scheduler queue is full")]
    QueueFull,
    /// All workers have exited or the scheduler is shutting down.
    #[error("engine scheduler is unavailable")]
    Unavailable,
    /// The admitted job panicked. The panic was contained (the worker was kept
    /// alive) and reported as this typed failure instead of a generic
    /// availability error.
    #[error("engine scheduler job panicked")]
    Panicked,
}

/// Small bounded worker pool used by the embedded engine facade.
pub struct BoundedScheduler {
    sender: Option<SyncSender<Job>>,
    workers: Vec<JoinHandle<()>>,
    tenant_usage: Arc<Mutex<HashMap<TenantId, usize>>>,
    max_per_tenant: usize,
    inflight: Arc<AtomicUsize>,
    panics: Arc<AtomicUsize>,
    queue_capacity: usize,
}

impl BoundedScheduler {
    /// Create a scheduler with `workers` threads and `queue_capacity` pending
    /// jobs. Zero values are rejected so the scheduler cannot accept work it
    /// has no ability to service.
    ///
    /// # Errors
    /// Returns [`SchedulerError::Unavailable`] when either capacity is zero.
    pub fn new(workers: usize, queue_capacity: usize) -> Result<Self, SchedulerError> {
        Self::new_with_tenant_limit(workers, queue_capacity, queue_capacity)
    }

    /// Create a scheduler with an explicit per-tenant queued/in-flight cap.
    /// The cap is checked before the global queue, so one tenant cannot
    /// consume every pending slot while other tenants are admitted.
    ///
    /// # Errors
    /// Returns [`SchedulerError::Unavailable`] when any capacity is zero.
    pub fn new_with_tenant_limit(
        workers: usize,
        queue_capacity: usize,
        max_per_tenant: usize,
    ) -> Result<Self, SchedulerError> {
        if workers == 0 || queue_capacity == 0 || max_per_tenant == 0 {
            return Err(SchedulerError::Unavailable);
        }
        let (sender, receiver) = mpsc::sync_channel::<Job>(queue_capacity);
        let receiver = std::sync::Arc::new(std::sync::Mutex::new(receiver));
        let panics = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let receiver = std::sync::Arc::clone(&receiver);
            let panics = Arc::clone(&panics);
            handles.push(thread::spawn(move || loop {
                let job = receiver.lock().ok().and_then(|guard| guard.recv().ok());
                let Some(job) = job else { break };
                // A request is untrusted work from the scheduler's point of
                // view. Keep a panic in one job from silently reducing the
                // worker pool and turning a transient bug into an outage.
                if catch_unwind(AssertUnwindSafe(job)).is_err() {
                    panics.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        Ok(Self {
            sender: Some(sender),
            workers: handles,
            tenant_usage: Arc::new(Mutex::new(HashMap::new())),
            max_per_tenant,
            inflight: Arc::new(AtomicUsize::new(0)),
            panics,
            queue_capacity,
        })
    }

    /// Execute a job on a bounded worker and wait for its result. Admission
    /// is nonblocking: callers receive `QueueFull` instead of extending an
    /// unbounded queue under load.
    ///
    /// # Errors
    /// Returns [`SchedulerError::QueueFull`] when no queue slot is available,
    /// or [`SchedulerError::Unavailable`] after worker shutdown.
    pub fn execute<R: Send + 'static>(
        &self,
        job: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, SchedulerError> {
        let sender = self.sender.as_ref().ok_or(SchedulerError::Unavailable)?;
        self.inflight.fetch_add(1, Ordering::Relaxed);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let panics = Arc::clone(&self.panics);
        let task: Job = Box::new(move || {
            // Catch at the task boundary so a panicking job reports a typed
            // `Panicked` error to its caller; the worker keeps running.
            if let Ok(value) = catch_unwind(AssertUnwindSafe(job)) {
                let _ = result_tx.send(Ok(value));
            } else {
                panics.fetch_add(1, Ordering::Relaxed);
                let _ = result_tx.send(Err(SchedulerError::Panicked));
            }
        });
        let result = match sender.try_send(task) {
            Ok(()) => result_rx
                .recv()
                .map_err(|_| SchedulerError::Unavailable)
                .and_then(|result| result),
            Err(TrySendError::Full(_)) => Err(SchedulerError::QueueFull),
            Err(TrySendError::Disconnected(_)) => Err(SchedulerError::Unavailable),
        };
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        result
    }

    /// Execute work with a per-tenant queued/in-flight admission limit.
    ///
    /// # Errors
    /// Returns [`SchedulerError::QueueFull`] when either the tenant or global
    /// queue is at capacity, or [`SchedulerError::Unavailable`] after
    /// shutdown.
    pub fn execute_for_tenant<R: Send + 'static>(
        &self,
        tenant: TenantId,
        job: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, SchedulerError> {
        let sender = self.sender.as_ref().ok_or(SchedulerError::Unavailable)?;
        {
            let mut usage = self
                .tenant_usage
                .lock()
                .map_err(|_| SchedulerError::Unavailable)?;
            let current = usage.get(&tenant).copied().unwrap_or(0);
            if current >= self.max_per_tenant {
                return Err(SchedulerError::QueueFull);
            }
            usage.insert(tenant, current + 1);
        }
        self.inflight.fetch_add(1, Ordering::Relaxed);
        let usage = Arc::clone(&self.tenant_usage);
        let panics = Arc::clone(&self.panics);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let task: Job = Box::new(move || {
            if let Ok(value) = catch_unwind(AssertUnwindSafe(job)) {
                let _ = result_tx.send(Ok(value));
            } else {
                panics.fetch_add(1, Ordering::Relaxed);
                let _ = result_tx.send(Err(SchedulerError::Panicked));
            }

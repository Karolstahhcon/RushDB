//! Per-tenant bulk share: at most `permits` bulk (ingest) requests
//! of one tenant run at a time; further ones wait in line before they take a
//! worker. Every tenant's commits go through one publisher, so a tenant
//! loading millions of records from many writers otherwise queues every
//! other tenant's commits behind its batches (measured 2026-10-17: light
//! tenants' commit p50 27 ms alone, 1.03 s beside a bulk load). One tenant's
//! commits serialize anyway, so a small share costs a lone bulk load little.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

type Scopes = Arc<Mutex<HashMap<String, Arc<Semaphore>>>>;

/// Bulk permits per tenant scope.
pub(crate) struct BulkShare {
    permits: usize,
    scopes: Scopes,
}

/// A held bulk permit; dropping it admits the next waiter of its tenant.
pub(crate) struct BulkPermit {
    permit: Option<OwnedSemaphorePermit>,
    scope: String,
    scopes: Scopes,
    permits: usize,
}

impl BulkShare {
    /// `permits` bulk requests per tenant; `0` = unlimited.
    pub(crate) fn new(permits: usize) -> Self {
        Self {
            permits,
            scopes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Wait for a bulk permit of `scope`. `None` when the share is unlimited
    /// (or its lock is poisoned: the share then stops limiting rather than

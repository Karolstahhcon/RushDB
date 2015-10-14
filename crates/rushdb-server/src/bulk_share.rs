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
    /// refusing ingest).
    pub(crate) async fn acquire(&self, scope: &str) -> Option<BulkPermit> {
        if self.permits == 0 {
            return None;
        }
        let semaphore = {
            let Ok(mut scopes) = self.scopes.lock() else {
                tracing::error!("bulk share lock poisoned; ingest admitted without a share");
                return None;
            };
            Arc::clone(
                scopes
                    .entry(scope.to_owned())
                    .or_insert_with(|| Arc::new(Semaphore::new(self.permits))),
            )
        };
        let permit = semaphore.acquire_owned().await.ok()?;
        Some(BulkPermit {
            permit: Some(permit),
            scope: scope.to_owned(),
            scopes: Arc::clone(&self.scopes),
            permits: self.permits,
        })
    }

    /// Tenants with a live share entry (a holder or a waiter).
    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.scopes.lock().map_or(0, |scopes| scopes.len())
    }
}

impl Drop for BulkPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        // Forget an idle tenant so the map stays bounded by active tenants:
        // every permit is back and nobody else holds the semaphore.
        if let Ok(mut scopes) = self.scopes.lock() {
            let idle = scopes.get(&self.scope).is_some_and(|semaphore| {
                semaphore.available_permits() == self.permits && Arc::strong_count(semaphore) == 1

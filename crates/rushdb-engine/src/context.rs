//! Typed execution state shared by embedded engine callers.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rushdb_storage::{resources::ByteReservation, ReadView, StorageError, Store};
use rushdb_types::ids::TenantId;

/// Stable classifier used for admission, metrics, and maintenance policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationClass {
    /// A read-only query or metadata request.
    Read,
    /// A canonical graph mutation.
    Write,
    /// A multi-statement transaction operation.
    Transaction,
    /// Internal recovery, compaction, or retention work.
    Maintenance,
}

/// Errors raised before an operation reaches a transport-specific executor.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    /// The pinned read view could not be opened.
    #[error("open execution read view: {0}")]
    Storage(#[from] StorageError),
    /// The caller exceeded the context's byte budget.
    #[error("execution budget exceeded: requested {requested} bytes with {remaining} remaining")]
    Budget {
        /// Bytes requested by the rejected charge.
        requested: usize,
        /// Bytes still available before the rejected charge.
        remaining: usize,
    },
    /// The operation deadline elapsed.
    #[error("execution deadline exceeded")]
    Deadline,
    /// The context was created by a different store than the engine executing it.
    #[error("execution context belongs to a different store")]
    ForeignStore,
    /// The caller cancelled the pinned read view.
    #[error("execution cancelled")]
    Cancelled,
}

/// Typed request state owned by the engine boundary.
///
/// Product identity and project-to-scope mapping stay outside this type. The
/// engine receives the already-resolved tenant and enforces read-view,
/// deadline, and byte-budget invariants consistently for embedded callers.
pub struct ExecutionContext {
    tenant: TenantId,
    operation: OperationClass,
    read_view: Arc<ReadView>,
    deadline: Option<Instant>,
    byte_budget: Option<usize>,
    charged_bytes: usize,
    policy_generation: u64,
    transaction_id: Option<uuid::Uuid>,
    /// Shared store-level phase reservation held for this request lifetime.
    _phase_reservation: Option<ByteReservation>,
}

const PHASE_RESERVATION_BYTES: usize = 8 * 1024 * 1024;

impl ExecutionContext {
    /// Create a context over a pinned view of the current committed state.
    ///
    /// # Errors
    /// Returns [`ExecutionError::Storage`] when the read view cannot be opened.
    pub fn new(
        store: &Arc<Store>,
        tenant: TenantId,
        operation: OperationClass,
        deadline: Option<Duration>,
        byte_budget: Option<usize>,
    ) -> Result<Self, ExecutionError> {
        let phase_reservation = store
            .phase_budget()
            .map(|budget| {
                let available = budget.available();
                budget
                    .try_reserve(PHASE_RESERVATION_BYTES)
                    .map_err(|_| ExecutionError::Budget {
                        requested: PHASE_RESERVATION_BYTES,
                        remaining: available,
                    })
            })
            .transpose()?;
        Ok(Self {
            tenant,
            operation,
            read_view: Arc::new(store.read_view_for_tenant(&tenant)?),
            deadline: deadline.map(|duration| Instant::now() + duration),
            byte_budget,
            charged_bytes: 0,
            policy_generation: 0,
            transaction_id: None,
            _phase_reservation: phase_reservation,
        })
    }

    /// Resolved tenant enforced by this context.
    #[must_use]
    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// Operation classifier used by admission and metrics.
    #[must_use]
    pub fn operation(&self) -> OperationClass {
        self.operation
    }

    /// Pinned read view shared by all engine reads in this request.
    #[must_use]
    pub fn read_view(&self) -> &Arc<ReadView> {
        &self.read_view
    }

    /// Deadline instant captured by this context, when one was configured.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Set the policy generation captured by the caller's authorization step.
    pub fn set_policy_generation(&mut self, generation: u64) {
        self.policy_generation = generation;
    }

    /// Policy generation captured for this request.
    #[must_use]
    pub fn policy_generation(&self) -> u64 {
        self.policy_generation
    }

    /// Attach a transaction identity after the transaction coordinator admits it.
    pub fn set_transaction_id(&mut self, transaction_id: uuid::Uuid) {
        self.transaction_id = Some(transaction_id);
    }

    /// Transaction identity, when this context is transaction-bound.
    #[must_use]
    pub fn transaction_id(&self) -> Option<uuid::Uuid> {
        self.transaction_id
    }

    /// Check the deadline before entering another engine phase.
    ///
    /// # Errors
    /// Returns [`ExecutionError::Deadline`] after the configured deadline.
    pub fn check_deadline(&self) -> Result<(), ExecutionError> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(ExecutionError::Deadline);
        }
        Ok(())
    }

    /// Check caller cancellation and the deadline before entering work.
    ///
    /// # Errors
    /// Returns [`ExecutionError::Cancelled`] when the caller cancelled the
    /// read view, or [`ExecutionError::Deadline`] after the deadline.
    pub fn check_active(&self) -> Result<(), ExecutionError> {
        self.read_view
            .check_cancelled()
            .map_err(|_| ExecutionError::Cancelled)?;
        self.check_deadline()
    }

    /// Return whether this context was opened from `store`.
    #[must_use]
    pub fn belongs_to(&self, store: &Store) -> bool {
        self.read_view.belongs_to(store)
    }

    /// Charge bounded working bytes and check the deadline atomically with the
    /// admission decision. Failed charges leave the prior charge unchanged.
    ///
    /// # Errors
    /// Returns [`ExecutionError::Deadline`] when expired or
    /// [`ExecutionError::Budget`] when the charge exceeds the limit.
    pub fn charge_bytes(&mut self, bytes: usize) -> Result<(), ExecutionError> {
        self.check_deadline()?;
        let next = self.charged_bytes.saturating_add(bytes);
        if let Some(limit) = self.byte_budget {
            if next > limit {
                return Err(ExecutionError::Budget {
                    requested: bytes,
                    remaining: limit.saturating_sub(self.charged_bytes),
                });
            }
        }
        self.charged_bytes = next;
        Ok(())
    }

    /// Release previously charged working bytes, used by rollback paths.
    pub fn release_bytes(&mut self, bytes: usize) {
        self.charged_bytes = self.charged_bytes.saturating_sub(bytes);
    }

    /// Current charged working bytes.
    #[must_use]
    pub fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }

    /// Remaining caller budget for an inner query working-set admission.
    #[must_use]
    pub fn remaining_budget(&self) -> Option<usize> {
        self.byte_budget
            .map(|limit| limit.saturating_sub(self.charged_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushdb_storage::resources::StorageResources;

    #[test]
    fn context_charges_atomically_and_releases_on_rollback() {

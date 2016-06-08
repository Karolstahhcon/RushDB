//! Shared managed-byte accounting. Reservations are ownership tokens, not
//! allocations, an RSS limit, or a substitute for charging each consumer.
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex, MutexGuard,
};
use thiserror::Error;

/// Errors from managed-byte accounting: invalid budgets, exhausted limits,
/// and reservation ownership violations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResourceError {
    /// The requested budget or limit is invalid.
    #[error("invalid resource budget: {0}")]
    Invalid(&'static str),
    /// A budget could not admit the requested bytes.
    #[error("resource budget `{budget}` exhausted: requested {requested} bytes, used {used}, limit {limit}")]
    Exhausted {
        /// Budget that refused the request.
        budget: &'static str,
        /// Bytes requested.
        requested: usize,
        /// Bytes currently charged.
        used: usize,
        /// Configured byte limit.
        limit: usize,
    },
    /// The two reservations belong to different resource trees.
    #[error("reservation belongs to a different resource tree")]
    DifferentTree,
    /// A reservation operation would exceed the bytes it owns.
    #[error("reservation operation exceeds its owned bytes")]
    InvalidOwnership,
}

/// Point-in-time usage of one budget: configured limit, current and peak use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetSnapshot {
    /// Configured byte limit.
    pub limit: usize,
    /// Bytes currently charged.
    pub used: usize,
    /// Highest usage observed.
    pub peak: usize,
}

/// Admission error for a [`FairAdmission::acquire`] attempt. A successful call
/// returns the owned [`ByteReservation`]; failure returns one of these without
/// charging bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    /// The deadline expired before bytes were available; no charge was made.
    TimedOut,
    /// The gate was cancelled before admission; no charge was made.
    Cancelled,
    /// The budget could not admit the request at this instant.
    Exhausted,
}

/// Monotonic ticket source for fair-admission waiters.
static NEXT_TICKET: AtomicU64 = AtomicU64::new(1);

struct Node {
    name: &'static str,
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    depth: usize,
    parent: Option<Arc<Node>>,
    ledger: Arc<Mutex<()>>,
}

/// Child usage is included in every ancestor; creating children does not
/// multiply the parent limit. Names are static component names, not tenant
/// identifiers or unbounded metric labels. Dropping the last child handle and
/// reservation releases its metadata without retaining a central registry.
#[derive(Clone)]
pub struct ByteBudget(Arc<Node>);

impl ByteBudget {
    /// Create a root budget labelled `name` with the given byte `limit`.
    ///
    /// # Errors
    /// Returns [`ResourceError::Invalid`] if `limit` is zero.
    pub fn new(name: &'static str, limit: usize) -> Result<Self, ResourceError> {
        if limit == 0 {
            return Err(ResourceError::Invalid("limit must be positive"));
        }
        Ok(Self(Arc::new(Node {
            name,
            limit,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            depth: 0,
            parent: None,
            ledger: Arc::new(Mutex::new(())),
        })))
    }

    /// Create a child budget whose usage is also charged to this budget.
    ///
    /// # Errors
    /// Returns [`ResourceError::Invalid`] if `limit` is zero or exceeds the
    /// parent limit, or if nesting would exceed 32 levels.
    pub fn child(&self, name: &'static str, limit: usize) -> Result<Self, ResourceError> {
        if limit == 0 || limit > self.0.limit {
            return Err(ResourceError::Invalid(
                "child limit must be positive and no greater than parent",
            ));
        }
        if self.0.depth >= 32 {
            return Err(ResourceError::Invalid("budget nesting exceeds 32 levels"));
        }
        Ok(Self(Arc::new(Node {
            name,
            limit,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            depth: self.0.depth + 1,
            parent: Some(self.0.clone()),
            ledger: self.0.ledger.clone(),
        })))
    }

    /// Serialize *growth* (validate + add) across the whole budget tree.
    /// Releases and reads are lock-free atomics, so this lock is held only on
    /// the admission path and never blocks a `Drop`. Recover a poisoned mutex
    /// so a panicked admission cannot wedge future reservations.
    fn lock(&self) -> MutexGuard<'_, ()> {
        self.0
            .ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn ancestors(&self) -> impl Iterator<Item = &Node> {
        std::iter::successors(Some(self.0.as_ref()), |node| node.parent.as_deref())
    }

    fn validate_add(&self, bytes: usize) -> Result<(), ResourceError> {
        for node in self.ancestors() {
            let used = node.used.load(Ordering::Acquire);
            if bytes > node.limit - used {
                return Err(ResourceError::Exhausted {
                    budget: node.name,
                    requested: bytes,
                    used,
                    limit: node.limit,
                });
            }
        }
        Ok(())
    }

    fn add(&self, bytes: usize) {
        for node in self.ancestors() {
            let used = node.used.fetch_add(bytes, Ordering::AcqRel) + bytes;
            node.peak.fetch_max(used, Ordering::Relaxed);
        }
    }

    /// Release a charge. Atomic and lock-free: a release can only lower usage,
    /// so it cannot race a concurrent admission into over-subscription. Callers
    /// hold `ledger` only when growing.
    fn subtract(&self, bytes: usize) {
        for node in self.ancestors() {
            node.used.fetch_sub(bytes, Ordering::Release);
        }
    }

    /// Snapshot this budget's limit, current usage, and peak usage.
    #[must_use]
    pub fn snapshot(&self) -> BudgetSnapshot {
        BudgetSnapshot {
            limit: self.0.limit,
            used: self.0.used.load(Ordering::Acquire),
            peak: self.0.peak.load(Ordering::Relaxed),
        }
    }

    /// Available in this budget AND every parent at one accounting instant.
    ///
    /// # Panics
    /// Panics if the ancestor chain is empty, which cannot happen because a
    /// budget always has at least itself.
    #[must_use]
    pub fn available(&self) -> usize {
        self.ancestors()
            .map(|node| node.limit - node.used.load(Ordering::Acquire))
            .min()
            .unwrap()
    }

    /// Immediate admission, before allocation. Queueing, deadlines and fair
    /// scheduling are deliberately separate work; this never blocks for bytes.
    ///
    /// # Errors
    /// Returns [`ResourceError::Exhausted`] if this budget or any ancestor
    /// cannot admit `bytes`.
    pub fn try_reserve(&self, bytes: usize) -> Result<ByteReservation, ResourceError> {
        let _guard = self.lock();
        self.validate_add(bytes)?;
        self.add(bytes);
        Ok(ByteReservation {
            budget: self.clone(),
            bytes,
        })
    }
}

/// Not Clone: exactly one owner releases each charge, including during unwind.
/// Move this with the allocated buffer; do not drop it while retaining data.
pub struct ByteReservation {
    budget: ByteBudget,
    bytes: usize,
}

impl ByteReservation {
    /// Bytes currently owned by this reservation.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Change the owned byte count, charging or releasing the difference.
    ///
    /// # Errors
    /// Returns [`ResourceError::Exhausted`] if growing the reservation cannot
    /// be admitted.
    pub fn resize(&mut self, bytes: usize) -> Result<(), ResourceError> {
        if bytes > self.bytes {
            // Growing must be atomic against other admissions.
            let _guard = self.budget.lock();
            self.budget.validate_add(bytes - self.bytes)?;
            self.budget.add(bytes - self.bytes);
        } else {
            // Shrinking only releases; lock-free.
            self.budget.subtract(self.bytes - bytes);
        }
        self.bytes = bytes;
        Ok(())
    }

    /// Split ownership without releasing or charging the same bytes twice.
    ///
    /// # Errors
    /// Returns [`ResourceError::InvalidOwnership`] if `bytes` exceeds the
    /// owned amount.
    pub fn split(&mut self, bytes: usize) -> Result<Self, ResourceError> {
        if bytes > self.bytes {
            return Err(ResourceError::InvalidOwnership);
        }
        self.bytes -= bytes;
        Ok(Self {
            budget: self.budget.clone(),
            bytes,
        })
    }

    /// Transfer a live charge atomically, including shared-parent accounting.
    /// Failure leaves the source reservation and all counters unchanged.
    ///
    /// # Errors
    /// Returns [`ResourceError::DifferentTree`] if `destination` is in another
    /// tree, or [`ResourceError::Exhausted`] if it cannot admit the bytes.
    pub fn transfer(&mut self, destination: &ByteBudget) -> Result<(), ResourceError> {
        if !Arc::ptr_eq(&self.budget.0.ledger, &destination.0.ledger) {
            return Err(ResourceError::DifferentTree);
        }
        {
            let _guard = self.budget.lock();
            self.budget.subtract(self.bytes);
            if let Err(error) = destination.validate_add(self.bytes) {
                self.budget.add(self.bytes);
                return Err(error);
            }
            destination.add(self.bytes);
        }
        self.budget = destination.clone();
        Ok(())
    }
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        // Lock-free release: this is the hottest accounting path (every
        // reservation, cache eviction, and rollback).
        self.budget.subtract(self.bytes);
    }
}

/// Bounded fair admission gate: serializes requests for bytes from a
/// budget and admits them in arrival order when capacity frees up, honoring a
/// caller deadline and an external cancellation flag. `try_reserve` remains
/// the non-blocking primitive; this adds bounded queueing, deadlines, and
/// cancellation for work that must wait rather than fail immediately (e.g.
/// group-commit staging, transaction buffers).
///
/// Fairness is FIFO by arrival; a cancelled waiter releases its slot so a
/// blocked call cannot be starved by a never-completing predecessor.
pub struct FairAdmission {
    budget: ByteBudget,
    /// FIFO of queued waiters, each tracking the bytes it wants and its
    /// deadline (ms since epoch) / cancellation flag.
    queue: Mutex<std::collections::VecDeque<Waiter>>,
    /// Broadcast when bytes are released or a waiter is removed.
    changed: std::sync::Condvar,
}

struct Waiter {
    deadline_ms: u64,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    ticket: u64,
}

impl FairAdmission {
    /// Create a fair-admission gate over `budget`.
    #[must_use]

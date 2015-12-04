//! Retention bookkeeping: the consumer registry, the protected history floor,
//! embedding-slot reclamation, and durable tenant enumeration. The
//! maintenance tick enumerates tenants once and passes the list to every
//! consumer (C2, cloud-capacity F2).

use std::sync::Arc;

use rushdb_types::ids::TenantId;

use super::Node;

impl Node {
    /// Register or advance a consumer's history acknowledgement floor.
    ///
    /// # Errors
    /// [`ConsumerResyncRequired`](crate::retention::ConsumerResyncRequired)
    /// when retention already reclaimed `floor` (the consumer is lost and
    /// must rebuild from a fresh base).
    ///
    /// A poisoned retention registry lock is recovered, not propagated.
    pub fn acknowledge_retention_consumer(
        &self,
        consumer: impl Into<String>,
        floor: u64,
    ) -> Result<(), crate::retention::ConsumerResyncRequired> {
        let (acknowledged, floors) = {
            let mut consumers = self
                .retention_consumers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let acknowledged = consumers.acknowledge(consumer, floor, crate::retention::unix_ms());
            (acknowledged, consumers.snapshot())
        };
        if let Err(error) = self.persist_retention_consumers(&floors) {
            tracing::warn!(%error, "failed to persist retention consumer acknowledgement");
        }
        acknowledged
    }

    /// Whether retention passed `consumer` and it has not resynced.
    ///
    /// A poisoned retention registry lock is recovered, not propagated.
    #[cfg(test)]
    pub(crate) fn retention_consumer_lost(&self, consumer: &str) -> bool {
        self.retention_consumers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_lost(consumer)
    }

    /// Retention reclaimed everything below `floor` (hard cap): drop
    /// every consumer it passed, logging each, and persist the registry.
    ///
    /// A poisoned retention registry lock is recovered, not propagated.
    pub(crate) fn enforce_retention_floor(&self, floor: u64) {
        let (lost, floors) = {
            let mut consumers = self
                .retention_consumers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (consumers.enforce_floor(floor), consumers.snapshot())
        };
        if lost.is_empty() {
            return;
        }
        for consumer in &lost {
            tracing::warn!(
                consumer = consumer.as_str(),
                oldest_available_lsn = floor,
                "retention cap passed a consumer; it must resync"
            );
        }
        if let Err(error) = self.persist_retention_consumers(&floors) {
            tracing::warn!(%error, "failed to persist lost retention consumers");
        }
    }

    /// Release a consumer so it no longer protects retained history.
    ///
    /// A poisoned retention registry lock is recovered, not propagated.
    #[must_use]
    pub fn release_retention_consumer(&self, consumer: &str) -> bool {
        let (released, floors) = {
            let mut consumers = self
                .retention_consumers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let released = consumers.release(consumer);
            (released, consumers.snapshot())
        };
        if released {
            if let Err(error) = self.persist_retention_consumers(&floors) {
                tracing::warn!(%error, "failed to persist retention consumer release");
            }
        }
        released
    }

    /// Expire retention consumers whose lease lapsed without renewal, persist
    /// the registry, and return how many were dropped.
    /// Called by the maintenance tick before the retention floor is sampled.
    pub(crate) fn expire_retention_consumers(&self) -> usize {
        let (expired, floors) = {
            let mut consumers = self
                .retention_consumers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let expired = consumers.expire(crate::retention::unix_ms());
            (expired, consumers.snapshot())
        };
        if expired.is_empty() {
            return 0;
        }
        for consumer in &expired {
            tracing::warn!(
                consumer = consumer.as_str(),
                "retention consumer lease expired; it no longer pins the WAL"
            );
        }
        if let Err(error) = self.persist_retention_consumers(&floors) {
            tracing::warn!(%error, "failed to persist expired retention consumers");
        }
        expired.len()
    }

    /// Lowest acknowledged floor currently protected by a live consumer.
    ///
    /// Text indexes never pin the WAL: their applier resumes from
    /// pending rows, not from history, so the floor is the retention
    /// consumers' and live read pins' alone.
    ///
    /// A poisoned retention registry lock is recovered, not propagated.
    #[must_use]
    pub fn protected_retention_floor(&self) -> Option<u64> {
        self.consumer_and_pin_floor()
    }

    fn consumer_and_pin_floor(&self) -> Option<u64> {
        let consumer_floor = self
            .retention_consumers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .protected_floor();
        let read_pin_floor = self.store.retention_summary().0;
        [consumer_floor, read_pin_floor].into_iter().flatten().min()
    }


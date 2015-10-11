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

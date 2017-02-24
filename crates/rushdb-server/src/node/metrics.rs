//! Metrics registry and rendering, isolated from request dispatch.

use std::sync::Arc;

use prometheus::Encoder as _;

pub struct Metrics {
    registry: prometheus::Registry,
    requests: prometheus::IntCounterVec,
    duration: prometheus::HistogramVec,
    commits: prometheus::IntCounter,
    text_pending: prometheus::IntGaugeVec,
    /// Tenant labels currently emitted on `rushdb_records_stored`.
    record_tenant_labels: std::sync::Mutex<std::collections::BTreeSet<String>>,
    tenants_resident: prometheus::IntGauge,
    ann_exact_fallbacks: prometheus::IntCounter,
    unindexed_walks: prometheus::IntCounter,
    tenant_resident_bytes: prometheus::IntGauge,
    budget_used: prometheus::IntGaugeVec,
    budget_peak: prometheus::IntGaugeVec,
    budget_limit: prometheus::IntGaugeVec,
    rows: prometheus::IntCounterVec,
    records: prometheus::IntGaugeVec,
    bytes: prometheus::IntGaugeVec,
    commit_latency: prometheus::Histogram,
    commit_phase: prometheus::HistogramVec,
    alloc_allocated: prometheus::IntGauge,
    alloc_total: prometheus::IntCounter,
    alloc_active: prometheus::IntGauge,
    alloc_metadata: prometheus::IntGauge,
    alloc_resident: prometheus::IntGauge,
    alloc_retained: prometheus::IntGauge,
    storage_file_bytes: prometheus::IntGauge,
    storage_data_bytes: prometheus::IntGauge,
    storage_history_bytes: prometheus::IntGauge,
    storage_log_bytes: prometheus::IntGauge,
    storage_receipt_bytes: prometheus::IntGauge,
    storage_admission_waits: prometheus::IntCounter,
    storage_admission_wait_ms: prometheus::IntCounter,
    storage_checkpoints: prometheus::IntCounter,
    storage_checkpoint_wait_ms: prometheus::IntCounter,
    storage_backend_mutex_wait_ms: prometheus::IntCounter,
    storage_backend_mutex_hold_ms: prometheus::IntCounter,
    storage_backend_mutex_hold_max_ms: prometheus::IntGauge,
    /// C0 (cloud-capacity F12): WAL bytes retained on disk.
    wal_retained_bytes: prometheus::IntGauge,
    /// C0: bytes held by the in-memory conflict window.
    conflict_window_bytes: prometheus::IntGauge,
    /// C0: HTTP request-body bytes reserved in flight.
    http_inflight_body_bytes: prometheus::IntGauge,
    /// C0: duration of each maintenance (GC/retention/checkpoint) tick.
    gc_tick_seconds: prometheus::Histogram,
    retention_consumers: prometheus::IntGauge,
    retention_protected_floor: prometheus::IntGauge,
    retention_oldest_reader: prometheus::IntGauge,
    sequencer_inflight_keys: prometheus::IntGauge,
    planner_stats_bytes: prometheus::IntGauge,
    planner_stats_entries: prometheus::IntGauge,
    group_commits_total: prometheus::IntCounter,
    scheduler_inflight: prometheus::IntGauge,
    scheduler_capacity: prometheus::IntGauge,
    log_rows_pruned: prometheus::IntGauge,
    receipts_swept: prometheus::IntGauge,
    compactions_total: prometheus::IntCounter,
    work: prometheus::IntCounterVec,
    boot_phase: prometheus::HistogramVec,
    maintenance_step: prometheus::HistogramVec,
    vector_searches: prometheus::IntCounterVec,
    writer_holds: std::sync::Arc<super::writer_holds::WriterHoldCollector>,
}

/// Registers a collector the metrics also keep a handle to.
struct SharedCollector<C>(std::sync::Arc<C>);

impl<C: prometheus::core::Collector> prometheus::core::Collector for SharedCollector<C> {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.0.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.0.collect()
    }
}

const BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

impl Metrics {
    pub(super) fn observe_state(
        &self,
        stats: Option<rushdb_storage::StorageStatistics>,
        consumers: &crate::retention::RetentionConsumers,
        scheduler: &rushdb_engine::BoundedScheduler,
    ) {
        if let Some(stats) = stats {
            let clamp = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
            self.storage_data_bytes.set(clamp(stats.data_bytes));
            self.storage_history_bytes.set(clamp(stats.history_bytes));
            self.storage_log_bytes.set(clamp(stats.log_bytes));
            self.storage_receipt_bytes.set(clamp(stats.receipt_bytes));
            advance(&self.storage_admission_waits, clamp(stats.admission_waits));
            advance(
                &self.storage_admission_wait_ms,
                clamp(stats.admission_wait_ms),
            );
            advance(&self.storage_checkpoints, clamp(stats.checkpoints));
            advance(
                &self.storage_checkpoint_wait_ms,
                clamp(stats.checkpoint_wait_ms),
            );
        }
        self.retention_consumers
            .set(i64::try_from(consumers.len()).unwrap_or(i64::MAX));
        self.retention_protected_floor.set(
            consumers
                .protected_floor()
                .map_or(0, |floor| i64::try_from(floor).unwrap_or(i64::MAX)),
        );
        self.scheduler_inflight
            .set(i64::try_from(scheduler.inflight()).unwrap_or(i64::MAX));
        self.scheduler_capacity
            .set(i64::try_from(scheduler.capacity()).unwrap_or(i64::MAX));
    }

    /// Records: an "all" series always; per-tenant series only when
    /// enabled, for the bounded set of recently served tenants. Labels that
    /// left that set (evicted or purged tenants) are removed.
    fn observe_records(
        &self,
        records: &std::collections::BTreeMap<u64, u64>,
        per_tenant: Option<&[rushdb_types::ids::TenantId]>,
    ) {
        let total: u64 = records.values().sum();
        self.records
            .with_label_values(&["all"])
            .set(i64::try_from(total).unwrap_or(i64::MAX));
        let desired: std::collections::BTreeSet<String> = per_tenant
            .unwrap_or_default()
            .iter()
            .map(|tenant| tenant.0.to_string())
            .collect();
        {
            let mut emitted = self
                .record_tenant_labels
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for stale in emitted.difference(&desired) {
                let _ = self.records.remove_label_values(&[stale]);
            }
            for label in &desired {
                let count = label
                    .parse::<u64>()
                    .ok()
                    .and_then(|tenant| records.get(&tenant))
                    .copied()
                    .unwrap_or(0);
                self.records
                    .with_label_values(&[label])
                    .set(i64::try_from(count).unwrap_or(i64::MAX));
            }
            *emitted = desired;
        }
    }

    /// Resident tenants and their charged bytes.
    pub(super) fn observe_residency(&self, tenants: usize, bytes: usize) {
        advance(
            &self.ann_exact_fallbacks,
            i64::try_from(rushdb_vector::ann::ann_exact_fallbacks_total()).unwrap_or(i64::MAX),
        );
        advance(
            &self.unindexed_walks,
            i64::try_from(rushdb_query::unindexed_walks_total()).unwrap_or(i64::MAX),
        );
        self.tenants_resident
            .set(i64::try_from(tenants).unwrap_or(i64::MAX));
        self.tenant_resident_bytes

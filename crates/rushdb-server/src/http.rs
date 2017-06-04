//! HTTP adapter for `RushDB`'s transport-neutral operation protocol.
//! Axum/Hyper own HTTP parsing, connection handling, limits, and tracing.

use std::net::TcpListener;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::node::Node;
use crate::retention::{over_rss_limit, MaintenanceConfig, RetentionPolicy};
use crate::{ApiError, ServerConfig};

/// P11 output budget: a successful response larger than this is rejected with
/// an explicit error rather than silently truncated. This bounds slow-client
/// and oversized-result memory at the transport boundary.
/// P11 output budget is `ServerConfig::max_request_bytes` (see `bounded_response`).

#[derive(Clone)]
struct AppState {
    cfg: ServerConfig,
    node: Arc<Node>,
    /// Node-wide budget for request bodies being read or executed.
    inflight_bodies: rushdb_storage::resources::ByteBudget,
    /// Bound on concurrently admitted protocol requests (scheduler workers +
    /// queue). Each admitted request holds a blocking-pool thread while it
    /// waits for the scheduler, so the bound also caps those threads; excess
    /// requests are shed immediately with a retryable 503.
    inflight_requests: Arc<tokio::sync::Semaphore>,
}

/// A bound HTTP protocol adapter: listener, router, and background workers.
pub struct HttpRouter {
    /// Resolved listen address (ephemeral port 0 becomes the assigned port).
    addr: std::net::SocketAddr,
    /// Pre-bound listener for the plaintext path (None when TLS is enabled —
    /// `axum-server` binds its own listener).
    listener: Option<TcpListener>,
    app: Router,
    node: std::sync::Weak<Node>,
    /// PEM certificate chain + private key paths when HTTPS is enabled.
    tls: Option<(std::path::PathBuf, std::path::PathBuf)>,
    /// Growth/memory maintenance policy resolved from `resources`.
    maintenance: MaintenanceConfig,
    /// HTTP worker threads; 0 = adapter default.
    threads: usize,
    /// History/GC sweep period (checkpoint interval).
    checkpoint_interval: std::time::Duration,
}

/// Boot a node from `cfg` and bind the HTTP adapter to its configured address.
///
/// # Errors
///
/// Returns a message when the node cannot bootstrap (for example an invalid
/// data directory) or the listener cannot bind the configured address.
pub fn new_router(mut cfg: ServerConfig) -> Result<(HttpRouter, u16), String> {
    let node = Arc::new(Node::bootstrap(&mut cfg)?);
    bind_router(cfg, node)
}

/// Bind the HTTP adapter around an already-bootstrapped node. `new_router`
/// is the usual entry point (bootstraps its own node); this is exposed
/// separately for callers that need their own handle to the same node
/// alongside the bound server (e.g. a test driving the node directly while
/// also exercising its HTTP surface).
///
/// # Errors
/// Returns a message when the listener cannot bind the configured address.
pub fn bind_router(cfg: ServerConfig, node: Arc<Node>) -> Result<(HttpRouter, u16), String> {
    let listener =
        TcpListener::bind((cfg.listen_addr.as_str(), cfg.port)).map_err(|e| e.to_string())?;
    bind_router_with_listener(cfg, node, listener)
}

/// [`bind_router`] over an already-bound socket — the one a
/// [`crate::boot::BootListener`] answered probes on while the node booted, so
/// the port is never closed between boot and serving.
///
/// # Errors
/// Returns a message when the socket cannot be configured.
pub fn bind_router_with_listener(
    cfg: ServerConfig,
    node: Arc<Node>,
    listener: TcpListener,
) -> Result<(HttpRouter, u16), String> {
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    let port = addr.port();
    let weak_node = Arc::downgrade(&node);
    let max_request_bytes = cfg.max_request_bytes;
    let tls = match (&cfg.tls_cert, &cfg.tls_key) {
        (Some(cert), Some(key)) => Some((cert.clone(), key.clone())),
        _ => None,
    };
    let maintenance = cfg.resources.maintenance(&cfg.token);
    let threads = cfg.resources.threads;
    let checkpoint_interval = cfg.resources.checkpoint_interval();
    let inflight_bodies = rushdb_storage::resources::ByteBudget::new(
        "http-inflight-bodies",
        cfg.resources
            .http_inflight_body_bytes
            .0
            .max(cfg.max_request_bytes),
    )
    .map_err(|e| e.to_string())?;
    let (workers, queue) = cfg.resources.scheduler_shape();
    let state = AppState {
        cfg,
        node,
        inflight_bodies,
        inflight_requests: Arc::new(tokio::sync::Semaphore::new(workers + queue)),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/protocol/v1/execute", post(execute))
        .route("/internal/replication/wal", get(replication_wal_tail))
        .route("/internal/replication/promote", post(replication_promote))
        .route("/internal/backup/base", post(backup_base_online))
        .route("/internal/usage", post(tenant_usage))
        .fallback(not_found)
        .layer(RequestBodyLimitLayer::new(max_request_bytes))
        // The `Bytes` extractor (and any body-reading extractor) is gated by
        // axum's `DefaultBodyLimit`, which defaults to 2MB — independent of the
        // tower-http layer above. Without raising it, batch imports >2MB were
        // rejected with 413 "length limit exceeded" despite the configured
        // body limit (`server.body-limit`, 16 MiB by default).
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    Ok((
        HttpRouter {
            addr,
            listener: Some(listener),
            app,
            node: weak_node,
            tls,
            maintenance,
            threads,
            checkpoint_interval,
        },
        port,
    ))
}

impl HttpRouter {
    /// Return the shared stop signal for all transports and maintenance work.
    pub fn shutdown_handle(&self) -> impl Fn() + 'static {
        let node = self.node.clone();
        move || {
            if let Some(node) = node.upgrade() {
                node.store().begin_shutdown();
            }
        }
    }
    /// Serve HTTP(S) until an unrecoverable error stops the listener, running
    /// the transaction reaper, GC, and optional compaction/watchdog workers.
    ///
    /// # Panics
    ///
    /// Panics if the multi-threaded Tokio runtime cannot be built, if the TLS
    /// certificate/key files cannot be loaded, or if the pre-bound listener
    /// cannot be adopted by Tokio.
    #[expect(
        clippy::too_many_lines,
        reason = "serve lifecycle keeps listener, maintenance task ownership, cancellation and final checkpoint together"
    )]
    /// # Errors
    /// Returns listener, checkpoint, or bounded shutdown failures.
    pub fn serve_forever(&mut self) -> Result<(), String> {
        let Some(listener) = self.listener.take() else {
            return Ok(());
        };
        let app = self.app.clone();
        let weak_node = self.node.clone();
        let tls = self.tls.clone();
        let self_addr = self.addr;
        let maintenance = self.maintenance.clone();
        let threads = self.threads;
        let checkpoint_interval = self.checkpoint_interval;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(if threads == 0 {
                std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
            } else {
                threads.max(1)
            })
            .build()
            .expect("HTTP runtime");
        let outcome = runtime.block_on(async move {
            let reaper =
                spawn_transaction_reaper(weak_node.clone(), std::time::Duration::from_secs(1));
            // Continuous incremental GC: bounded, resumable history sweeps and
            // policy-driven per-commit metadata retention.
            let gc = spawn_gc_worker(
                weak_node.clone(),
                checkpoint_interval,
                512,
                maintenance.retention,
                maintenance.keep_commits,
                maintenance.retention_bytes,
            );
            // Optional idle-window compaction (opt-in): operators choose when
            // the rewrite cost is acceptable.
            let compaction = maintenance.compact_interval.map(|period| {
                spawn_compaction_worker(
                    weak_node.clone(),
                    period,
                    maintenance.compact_quiesce,
                    maintenance.compact_growth_percent,
                )
            });
            // Durable byte accounting is refreshed off the request path so a
            // `/metrics` scrape never blocks on a full keyspace scan. The scan
            // is O(keyspace) CPU/IO, so on a large store it runs infrequently.
            let storage_stats =
                spawn_storage_stats_worker(weak_node.clone(), std::time::Duration::from_mins(15));
            // Index applier: applies pending text and structural
            // index changes with a ≤ 250 ms linger; idle tenants cost one
            // catalog read and never commit.
            let index_applier =
                spawn_index_applier(weak_node.clone(), std::time::Duration::from_millis(250));
            // Declared-index builds run continuously while any is pending.
            let index_builder = spawn_index_builder(weak_node.clone(), checkpoint_interval);
            // Continuous logical-WAL archiving: append-only segment files
            // plus a durable watermark, for off-box durability and PITR.
            let wal_archiver = maintenance.wal_archive_dir.clone().map(|dir| {
                spawn_wal_archiver(
                    weak_node.clone(),
                    std::time::Duration::from_secs(5),
                    dir,
                    maintenance.wal_archive_retention,
                    maintenance.wal_archive_retention_bytes,
                )
            });
            // Streaming replication follower: tails a
            // configured leader's WAL and replays it locally.
            let replication_follower = maintenance.replica.clone().map(|replica| {
                tokio::spawn(crate::replication::run_replication_follower(
                    weak_node.clone(),
                    replica,
                ))
            });
            // Kept for the shutdown path (the watchdog below consumes the other
            // handle).
            let shutdown_node = weak_node.clone();
            // Optional RSS watchdog (opt-in): when resident memory exceeds the
            // threshold, reclaim allocator pages and compact if idle.
            let watchdog = maintenance.rss_max_bytes.map(|limit| {
                spawn_memory_watchdog(
                    weak_node,
                    std::time::Duration::from_secs(10),
                    limit,
                    maintenance.compact_quiesce,
                )
            });
            let addr = self_addr;
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let tls_handle = axum_server::Handle::new();
            let tls_handle_for_server = tls_handle.clone();
            // Serve until an unrecoverable error OR a shutdown signal. On a
            // signal we fall through and drop the node, which closes the redb
            // file cleanly — an unclean kill makes redb repair (O(file)) on the
            // next open.
            let server = async move {
                if let Some((cert, key)) = tls {
                    tracing::debug!("HTTPS enabled on {addr}");
                    let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                        .await
                        .expect("load TLS certificate/key");
                    axum_server::tls_rustls::from_tcp_rustls(listener, config)
                        .handle(tls_handle_for_server)
                        .serve(app.into_make_service())
                        .await.map_err(|error| error.to_string())
                } else {
                    let listener =
                        tokio::net::TcpListener::from_std(listener).expect("HTTP listener");
                    axum::serve(listener, app)
                        .with_graceful_shutdown(async {
                            let _ = shutdown_rx.await;
                        })
                        .await.map_err(|error| error.to_string())
                }
            };
            tokio::pin!(server);
            let stop = async {
                tokio::select! {
                    () = shutdown_signal() => {},
                    () = async {
                        loop {
                            if shutdown_node.upgrade().is_none_or(|node| node.store().maintenance_aborted()) { break; }
                            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                        }
                    } => {},
                }
            };
            let mut outcome = tokio::select! {
                result = &mut server => result,
                () = stop => {
                    tracing::info!("shutdown signal received; closing the store cleanly");
                    // Stop in-flight maintenance scans so the store closes
                    // promptly instead of waiting for a full keyspace scan.
                    if let Some(node) = shutdown_node.upgrade() {
                        node.store().begin_shutdown();
                    }
                    let _ = shutdown_tx.send(());
                    tls_handle.graceful_shutdown(Some(std::time::Duration::from_secs(30)));
                    tokio::time::timeout(std::time::Duration::from_secs(30), &mut server)
                        .await.unwrap_or_else(|_| Err("HTTP transport drain deadline elapsed".into()))
                }
            };
            reaper.abort();
            gc.abort();
            storage_stats.abort();
            index_applier.abort();
            index_builder.abort();
            if let Some(archiver) = wal_archiver.as_ref() {
                archiver.abort();
            }
            if let Some(follower) = replication_follower.as_ref() {
                follower.abort();
            }
            if let Some(compaction) = compaction.as_ref() {
                compaction.abort();
            }
            if let Some(watchdog) = watchdog.as_ref() {
                watchdog.abort();
            }
            // Make shutdown deterministic: cancel maintenance, wait for every
            // task to observe cancellation, then flush buffered durability
            // before the owning Node/runtime is dropped.
            if let Some(node) = shutdown_node.upgrade() {
                node.store().begin_shutdown();
                // Aborting the loops above does not stop a closure already on
                // the blocking pool. Wait for every one to return (they observe
                // `begin_shutdown`) so no detached closure still holds the
                // Store when the runtime exits; otherwise redb is never closed
                // and the next open runs a full repair.
                if !node.maintenance_work().drained(std::time::Duration::from_secs(30)).await {
                    outcome = Err(format!(
                        "{} maintenance task(s) still running at the shutdown deadline",
                        node.maintenance_work().inflight()
                    ));
                }
                let checkpoint = tokio::task::spawn_blocking(move || node.store().checkpoint());
                match tokio::time::timeout(std::time::Duration::from_secs(5), checkpoint).await {
                    Ok(Ok(Ok(()))) => {},
                    failed => { outcome = Err(format!("final durability checkpoint failed: {failed:?}")); }
                }
            }
            let _ = reaper.await;
            let _ = gc.await;
            let _ = storage_stats.await;
            let _ = index_applier.await;
            let _ = index_builder.await;
            if let Some(archiver) = wal_archiver {
                let _ = archiver.await;
            }
            if let Some(follower) = replication_follower {
                let _ = follower.await;
            }
            if let Some(compaction) = compaction {
                let _ = compaction.await;
            }
            if let Some(watchdog) = watchdog {
                let _ = watchdog.await;
            }
            outcome
        });
        runtime.shutdown_timeout(std::time::Duration::from_secs(1));
        outcome
    }
}

pub(crate) fn spawn_transaction_reaper(
    node: std::sync::Weak<Node>,
    period: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(node) = node.upgrade() else {
                break;
            };
            // One sweep at a time, off the async executor; do not retain a
            // strong node reference while sleeping or create per-tenant tasks.
            if let Err(error) = node
                .maintenance_work()
                .clone()
                .spawn(move || node.reap_expired_transactions())
                .await
            {
                tracing::error!(%error, "transaction reaper stopped");
                break;
            }
        }
    })
}

/// Continuous incremental GC (W5, D8): a background loop drives bounded,
/// resumable `retire_before_batch` sweeps so history reclamation never
/// performs one monolithic redb txn. Each tick is a small write transaction
/// and a full sweep ends with a published per-shard checkpoint. The same tick
/// drives bounded per-commit metadata retention (command-log rows and outcome
/// payloads) so the durable file tracks live data instead of commit count.
pub(crate) fn spawn_gc_worker(
    node: std::sync::Weak<Node>,
    period: std::time::Duration,
    max_keys_per_tick: usize,
    retention: RetentionPolicy,
    keep_commits: u64,
    retention_bytes: Option<u64>,
) -> tokio::task::JoinHandle<()> {
    let sweep_budget = period / 5;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(node) = node.upgrade() else {
                break;
            };
            if let Some(acknowledged) = node.store().replication_acknowledged_commit() {
                if let Err(error) = node.acknowledge_retention_consumer("replica", acknowledged) {
                    tracing::error!(%error, "replication acknowledgement fell behind the retention cap");
                }
            }
            let result = node.maintenance_work().clone().spawn(move || {
                let tick_started = std::time::Instant::now();
                let observe_node = std::sync::Arc::clone(&node);
                let _observe = scopeguard_tick(move || {
                    observe_node.observe_gc_tick(tick_started.elapsed().as_secs_f64());
                });
                // Pull the store off the node's store and run one GC tick,
                // expiring abandoned read leases so history can advance.
                let store = node.store();
                // Each step is timed (`rushdb_maintenance_step_seconds`), so a
                // long tick names the step that held the writer.
                let step = std::time::Instant::now();
                store.expire_read_pins(node.read_lease_ms());
                // Planner statistics unused past their idle TTL go.
                store.planner_stats().evict_idle();
                // Freeze and merge ANN segments off the write path.
                let step = node.observe_maintenance_step("read_pins", step);
                node.maintain_vector_segments();
                let step = node.observe_maintenance_step("vector_segments", step);
                // Records-per-tenant gauge source: label counters, so cheap
                // enough to refresh twice a minute.
                node.refresh_records_by_tenant(std::time::Duration::from_secs(30));
                // C5: a consumer that stopped acknowledging (dead backup job,
                // removed replica, disabled archiver) must not pin the WAL.
                node.expire_retention_consumers();
                // Read-pin expiry must happen before sampling this floor;
                // otherwise one abandoned view can delay metadata pruning for
                // an extra maintenance interval.
                //
                // Tenants are enumerated ONCE per tick (an O(tenants)
                // skip-scan) and shared by every per-tenant step below.
                let tenants = match node.durable_tenants() {
                    Ok(tenants) => Some(tenants),
                    Err(error) => {
                        tracing::warn!(%error, "cannot enumerate tenants; protecting full history this tick");
                        None
                    }
                };
                let protected_floor = node.protected_retention_floor();
                // History reclamation in batches until the sweep completes or
                // a fifth of the tick period has passed.
                let step = node.observe_maintenance_step("housekeeping", step);
                let done = store.gc_tick_for(max_keys_per_tick, sweep_budget)?;
                let step = node.observe_maintenance_step("history_gc", step);
                // Retention runs after history GC so the sweep sees the newest
                // watermark. The commit floor is fixed for this tick; the loop
                // drains the bounded backlog. A byte target additionally prunes
                // the oldest command-log rows until the log fits.
                let floor = retention.capped_floor(
                    store.committed_version(),
                    keep_commits,
                    retention_bytes
                        .map(|max| store.log_byte_floor(max))
                        .transpose()?,
                    protected_floor,
                );
                // The caps are hard. Consumers the floor passed are
                // lost and must resync.
                node.enforce_retention_floor(floor);
                let mut pruned = 0usize;
                loop {
                    if store.maintenance_aborted() {
                        return Ok(done);
                    }
                    let n = store.retire_commit_metadata_before(floor, max_keys_per_tick)?;
                    pruned += n;
                    if n == 0 {
                        break;
                    }
                }
                if pruned > 0 {
                    tracing::debug!(pruned, "retention advanced past retired commits");

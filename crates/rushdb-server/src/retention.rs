//! Growth/memory maintenance policy, with Neo4j-style knobs. The canonical surface is
//! [`crate::resource_config::ResourceConfig::maintenance`] (config.yml /
//! `resources` section / environment via clap).

/// Retention policy for per-commit metadata (command-log rows and stored
/// outcome payloads). Commit-count based because the logical log has no
/// per-record timestamp; `keep_all` / `keep_none` mirror Neo4j's
/// `db.tx_log.rotation.retention_policy`.
///
/// **This governs commit metadata / WAL only, not MVCC history.** Superseded
/// key versions are retained for the *oldest live reader* and reclaimed
/// immediately when no reader is pinned; there is no configured history window.
/// The WAL/outcome window is legitimate because it feeds PITR, replication and
/// idempotent retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPolicy {
    /// Retain all commit metadata (no pruning).
    KeepAll,
    /// Retain only the current commit's metadata.
    KeepNone,
    /// Retain the most recent `N` commits.
    Commits(u64),
}

impl RetentionPolicy {
    /// Parse `keep_all`, `keep_none`, or `N commits` (the unit is optional and
    /// singular/plural tolerated). Byte/time policies are rejected explicitly
    /// because the logical log has no per-row size or timestamp index here.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when the policy is empty, malformed,
    /// uses a non-numeric count, or names an unsupported unit (for example a
    /// byte- or time-based retention policy).
    pub fn parse(raw: &str) -> Result<Self, String> {
        let text = raw.trim().to_ascii_lowercase();
        if text.is_empty() || text == "keep_all" {
            return Ok(Self::KeepAll);
        }
        if text == "keep_none" {
            return Ok(Self::KeepNone);
        }
        let mut parts = text.split_whitespace();
        let number = parts
            .next()
            .ok_or_else(|| "retention policy is empty".to_string())?;
        let unit = parts.next().unwrap_or("commits");
        if parts.next().is_some() {
            return Err(format!("invalid retention policy `{raw}`"));
        }
        let count: u64 = number
            .parse()
            .map_err(|_| format!("retention count `{number}` is not an integer"))?;
        match unit.trim_end_matches('s') {
            "commit" => Ok(Self::Commits(count)),
            other => Err(format!(
                "unsupported retention unit `{other}`; use `N commits`, `keep_all`, or `keep_none`"
            )),
        }
    }

    /// Lowest LSN that must be retained. `0` means "prune nothing".
    #[must_use]
    pub fn floor(&self, published: u64) -> u64 {
        match self {
            Self::KeepAll => 0,
            Self::KeepNone => published,
            Self::Commits(count) => published.saturating_sub(*count),
        }
    }

    /// The retention floor for one maintenance tick: commits
    /// below it lose their metadata and their WAL segments become
    /// reclaimable.
    ///
    /// The commit-count policy and the byte target are **hard caps**:
    /// consumers (replicas, backups, archivers, cursors) only protect
    /// history above them. A consumer that falls below is lost and must
    /// resync ([`ConsumerResyncRequired`]). Below the caps the floor follows
    /// the PostgreSQL rule (2026-10-10): the log keeps only what a consumer
    /// or open reader pins, plus an optional `keep` window of recent commits
    /// (default 0, like `wal_keep_size`). `KeepAll` sets no cap and prunes
    /// nothing.
    #[must_use]
    pub fn capped_floor(
        &self,
        published: u64,
        keep: u64,
        byte_floor: Option<u64>,
        protected_floor: Option<u64>,
    ) -> u64 {
        let wanted = published.saturating_sub(keep);
        let (hard, soft) = match self {
            Self::KeepAll => (0, 0),
            Self::KeepNone => (0, wanted),
            Self::Commits(_) => (self.floor(published), wanted),
        };
        let hard = hard.max(byte_floor.unwrap_or(0));
        let desired = soft.max(hard);
        protected_floor.map_or(desired, |protected| desired.min(protected).max(hard))
    }
}

/// A consumer acknowledged a commit that retention already reclaimed: it has
/// to rebuild from a fresh base (backup restore, snapshot export) and resume
/// at or after `oldest_available_lsn`. Protocol error code
/// `resync_required`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("resync required: retained history starts at LSN {oldest_available_lsn}")]
pub struct ConsumerResyncRequired {
    /// The oldest commit still retained.
    pub oldest_available_lsn: u64,
}

/// One consumer's retention claim: the lowest commit it still needs and when
/// it last acknowledged (unix milliseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerLease {
    /// Lowest commit the consumer still needs.
    pub floor: u64,
    /// When the consumer last acknowledged (unix milliseconds).
    pub acknowledged_at_ms: u64,
}

/// A bounded registry of retention consumers. Each consumer acknowledges the
/// lowest commit it still needs; the minimum acknowledgement protects all
/// consumers while allowing released consumers to stop pinning history.
///
/// Claims are **leases**. A consumer that stops acknowledging (a crashed

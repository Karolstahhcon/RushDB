//! Data-directory configuration record (`config.yml`).
//!
//! `config.yml` lives in the data directory and is written on first start. It
//! records the effective configuration that the directory was created under and
//! documents the whole `rushdb serve` option surface with commented defaults, so an
//! operator can inspect and adjust it without reading the CLI reference.
//!
//! Precedence:
//! - `data_dir`, `token` and `capability_secret` are always supplied by the
//!   CLI / environment (they are needed to locate the file, and secrets are
//!   never persisted here).
//! - Ports and resource/transaction sections are read from `config.yml` when present (the file
//!   is the source of truth for a data directory).
//! - The tenancy `mode` is an **isolation invariant**: an existing data
//!   directory cannot switch between single-tenant and multitenant modes. A
//!   mismatch is refused, because that would silently reinterpret the stored
//!   graph under a different isolation regime. To change the mode, migrate to a
//!   fresh data directory.

use serde::{Deserialize, Serialize};

use crate::ServerConfig;

/// Borrowed pair of resource sections serialized into the rendered file.
#[derive(Serialize)]
struct Limits<'a> {
    resources: &'a crate::resource_config::ResourceConfig,
    transactions: &'a crate::resource_config::TransactionConfig,
}

/// Schema version written to `config.yml`; mismatches refuse to boot.
pub const CONFIG_VERSION: u32 = 1;

/// File name of the configuration record inside a data directory.
pub const CONFIG_FILE_NAME: &str = "config.yml";

/// Versioned language enablement section rendered into `config.yml`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LanguageSection {
    languages: rushdb_protocol::LanguageSet,
}

/// The persisted configuration record. Security secrets are intentionally
/// absent: they are supplied at boot and never written to disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ConfigFile {
    /// Schema version of this record (see [`CONFIG_VERSION`]).
    pub version: u32,
    /// HTTP/Pgwire/gRPC/Bolt listener settings.
    pub server: ServerSection,
    /// Tenancy mode and HTTP scope header.
    pub tenancy: TenancySection,
    /// Absent in older v1 files: retain CLI/environment/embedded settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<crate::resource_config::ResourceConfig>,
    /// Transaction limits. Absent in older v1 files: retain the startup
    /// settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transactions: Option<crate::resource_config::TransactionConfig>,
    /// Startup query-language enablement. Absent = defaults
    /// (`searchquery` only; every statement language disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub languages: Option<rushdb_protocol::LanguageSet>,
}

/// Persisted listener settings (`server` section of `config.yml`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ServerSection {
    /// HTTP protocol adapter port.
    pub port: u16,
    /// HTTP bind address (default `127.0.0.1`). Absent = retain the startup
    /// setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen_addr: Option<String>,
    /// PEM certificate chain path for HTTPS. Both cert and key are required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<String>,
    /// PEM private key path (PKCS#8) for HTTPS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_key: Option<String>,
    /// PostgreSQL wire-protocol port.
    pub pgwire_port: u16,
    /// Maintenance mode: rejects writes and marks `/health` degraded.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub maintenance: bool,
    /// Gate `/ready` on vector-index warmup (no unindexed/stale bindings).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub require_index_warmup: bool,
    /// gRPC transport port (`0` = disabled). Absent in older v1 files:
    /// retain the startup setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_port: Option<u16>,
    /// Bolt (Neo4j wire) transport port (`0` = disabled). Absent in older v1
    /// files: retain the startup setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bolt_port: Option<u16>,
    /// Maximum protocol payload (HTTP request body, gRPC per-message, response
    /// budget). Bytes or B/KiB/MiB/GiB. Absent = 16MiB default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_limit: Option<crate::resource_config::ByteSize>,
    /// Persistent database directory (informational; the running server gets
    /// it from the CLI / `RUSHDB_DATA_DIR`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,
}

/// Persisted tenancy settings (`tenancy` section of `config.yml`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TenancySection {
    /// `single` or `multitenant`. Isolation invariant — see module docs.
    pub mode: String,
}

impl ConfigFile {
    fn mode_string(multitenant: bool) -> &'static str {
        if multitenant {
            "multitenant"
        } else {
            "single"
        }
    }

    /// Render a commented `config.yml` for the effective configuration.
    ///
    /// # Panics
    ///
    /// Panics only if a numeric resource setting or language set cannot be
    /// serialized to YAML, which cannot happen for the plain numeric/bool
    /// fields in these sections.
    #[must_use]
    pub fn render(cfg: &ServerConfig) -> String {
        let mode = Self::mode_string(cfg.multitenant);
        let data_dir = cfg
            .data_dir
            .as_ref()
            .map(|d| d.display().to_string())
            .unwrap_or_default();
        let data_dir_line = if data_dir.is_empty() {
            "  # data-dir: (in-memory node)".to_string()
        } else {
            // A JSON string is a YAML-compatible quoted scalar, including
            // paths containing colons, hashes, quotes, or newlines.
            format!("  data-dir: {}", serde_json::to_string(&data_dir).unwrap())
        };
        #[allow(clippy::format_in_format_args)]
        let mut rendered = format!(
            "# RushDB engine configuration.\n\
             # Generated on first start. Edit to change and restart to apply.\n\
             #\n\
             # Security secrets are never stored here. Set them via the CLI or\n\
             # environment:\n\
             #   token:             --token / RUSHDB_TOKEN\n\
             #   capability-secret: --capability-secret (HMAC secret enabling\n\
             #                       signed X-RushDB-Token capability auth)\n\
             #\n\
             # The tenancy mode is an isolation invariant: an existing data\n\
             # directory cannot switch between single-tenant and multitenant\n\
             # modes. To change it, migrate to a fresh data directory.\n\
             version: {}\n\
             server:\n\
             #   HTTP protocol adapter port.\n\
             {}\n\
             #   HTTP bind address (default 127.0.0.1). Use 0.0.0.0 to expose\n\
             #   beyond loopback (pair with TLS).\n\
             {}\n\
             #   HTTPS PEM certificate chain path (requires tls-key too).\n\
             {}\n\
             #   HTTPS PEM private key path (PKCS#8).\n\
             {}\n\
             #   PostgreSQL wire-protocol port.\n\
             {}\n\
             #   gRPC transport port (0 = disabled).\n\
             {}\n\
             #   Bolt (Neo4j wire) transport port (0 = disabled).\n\
             {}\n\
             #   Maximum protocol payload (HTTP request body, gRPC per-message,\n\
             #   response budget). Bytes or B/KiB/MiB/GiB. Default 16MiB.\n\
             {}\n\
             #   Maintenance mode (default false): reject writes and mark\n\
             #   /health as maintenance until cleared.\n\
             {}\n\
             #   Persistent database directory.\n\
             {}\n\
             tenancy:\n\
             #   single | multitenant\n\
             {}\n",
            CONFIG_VERSION,
            format!("  port: {}", cfg.port),
            format!("  listen-addr: {}", cfg.listen_addr),
            cfg.tls_cert.as_ref().map_or_else(
                || "  # tls-cert: (TLS disabled)".to_string(),
                |c| format!("  tls-cert: {}", serde_json::to_string(c).unwrap())
            ),
            cfg.tls_key.as_ref().map_or_else(
                || "  # tls-key: (TLS disabled)".to_string(),
                |k| format!("  tls-key: {}", serde_json::to_string(k).unwrap())
            ),
            format!("  pgwire-port: {}", cfg.pgwire_port),
            format!("  grpc-port: {}", cfg.grpc_port),
            format!("  bolt-port: {}", cfg.bolt_port),
            format!("  body-limit: {}B", cfg.max_request_bytes),
            format!("  maintenance: {}", cfg.maintenance),
            data_dir_line,
            format!("  mode: {mode}"),
        );
        rendered.push_str("# This file is the source of truth for this data directory: a startup flag or\n# environment variable that disagrees with a value here is a startup error.\n# Byte values are bytes; B/KiB/MiB/GiB strings are also accepted.\n# Managed reservations are NOT a process RSS cap.\n");
        rendered.push_str(
            &serde_yml::to_string(&Limits {
                resources: &cfg.resources,
                transactions: &cfg.transactions,
            })
            .expect("serializing numeric resource settings"),
        );
        // Languages section: searchquery is always enabled and not listed.
        let languages = cfg.languages;
        rendered.push('\n');
        rendered.push_str(
            &serde_yml::to_string(&LanguageSection { languages })
                .expect("serializing language settings"),
        );
        rendered.push('\n');
        rendered.push_str(
            "# searchquery (the native query document) is always enabled and cannot be disabled.\n",
        );
        rendered
    }

    /// Load and parse `config.yml` from the data directory, if it exists.
    ///
    /// # Errors
    ///
    /// Returns a message when the file exists but cannot be read, is not UTF-8,
    /// or does not parse as a [`ConfigFile`].
    pub fn load(data_dir: &std::path::Path) -> Result<Option<ConfigFile>, String> {
        let path = data_dir.join(CONFIG_FILE_NAME);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)
            .map_err(|error| format!("read config `{}`: {error}", path.display()))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|error| format!("config `{}` is not UTF-8: {error}", path.display()))?;
        serde_yml::from_str(text)
            .map(Some)
            .map_err(|error| format!("invalid config `{}`: {error}", path.display()))
    }
}

/// Write the config record into the data directory.
///
/// # Errors
///
/// Returns a message when the rendered `config.yml` cannot be written.
pub fn write_config(data_dir: &std::path::Path, cfg: &ServerConfig) -> Result<(), String> {
    let path = data_dir.join(CONFIG_FILE_NAME);
    std::fs::write(&path, ConfigFile::render(cfg))
        .map_err(|error| format!("write config `{}`: {error}", path.display()))
}

/// Rewrite `config.yml` without the node's replica role (after promotion, or
/// for a directory restored from another node's backup) and, when
/// `drop_archive` is set, without its WAL archive directory (a restored node
/// must not append to its source's archive). `cfg` is the effective config.
///
/// # Errors
/// Returns a message when `config.yml` cannot be written.
pub fn persist_without_roles(
    data_dir: &std::path::Path,
    cfg: &ServerConfig,
    drop_archive: bool,
) -> Result<(), String> {
    let mut detached = cfg.clone();
    detached.resources.replica_of = None;
    detached.resources.replica_token = None;
    if drop_archive {
        detached.resources.wal_archive_dir = None;
    }
    write_config(data_dir, &detached)
}

/// Settings that are this boot's role assignment rather than shared
/// directory configuration: an explicit flag sets them without being a
/// conflict with `config.yml` (which keeps them for flagless restarts).
const ROLE_FLAGS: [&str; 3] = ["replica-of", "replica-id", "replica-token"];

/// Validate an existing config record against the requested configuration.
///
/// The tenancy mode is the isolation invariant: it must match exactly.
/// `port`/`pgwire_port` are applied back onto the requested config (the file is
/// authoritative for a data directory). Secrets and `data_dir` are never read

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

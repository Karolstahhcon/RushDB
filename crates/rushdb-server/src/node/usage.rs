//! Per-tenant usage figures for a list of tenants, an operator read.
//!
//! The engine has no workspace, plan or billing concept: the caller names the
//! tenant scopes it wants grouped and gets one row per scope plus their sum.
//! Every figure is a counter maintained by the commit that changes it, so the
//! read costs one `get` per metric and embedding space for each tenant and
//! never scans tenant data.

use rushdb_lmpg::derived::{tenant_usage, TenantUsage};
use rushdb_types::ids::TenantScope;
use serde_json::{json, Value};

use super::Node;

/// Why a usage read failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageError {
    /// The request does not fit the node's tenancy mode or limits.
    Invalid(String),
    /// The store read failed.
    Store(String),
}

impl std::fmt::Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) | Self::Store(message) => f.write_str(message),
        }
    }
}

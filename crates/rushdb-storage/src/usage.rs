//! Per-tenant logical byte accounting.
//!
//! Every backend write already reads the value it replaces (for MVCC history),
//! so the change in a tenant's stored `key + value` bytes is known at apply
//! time at no extra read. A backend folds each op of a batch into
//! [`UsageBytes`] and writes one [`usage_bytes_key`] row per touched tenant in
//! the same backend transaction, so the figure is exact at every commit index
//! and replay (WAL, replicas) reproduces it.
//!
//! The figure is *logical*: it sums the row bytes the engine stores, not the
//! file pages that hold them (redb pages run partly full, so the file is
//! larger). The usage row itself is excluded, which keeps it a fixed point.

use std::collections::BTreeMap;


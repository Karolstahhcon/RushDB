//! Tenant directory: scope digest ↔ 4-byte tenant ordinal.
//!
//! The rows live in the system namespace and are written by an ordinary
//! commit (`Mutation::RegisterTenant`, expanded by LMPG), so registration is
//! logged, replicated and replayed like any other write. Ordinals are never
//! reused: a purged tenant keeps its rows with state `Purged`, and a later
//! registration of the same scope gets a fresh ordinal, so leftovers of the
//! old ordinal can never become visible. This module owns the encoding and a
//! bounded resolution cache.

use std::collections::HashMap;

use rushdb_types::ids::{TenantId, TenantScope};
use rushdb_types::keys;

use crate::{StorageError, Store};

/// Maximum cached scope → ordinal entries; the cache is cleared when full
/// (entries are re-read from disk on the next miss).
const CACHE_ENTRIES: usize = 65_536;

/// Lifecycle state of a directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantState {
    /// Serving reads and writes.
    Active,
    /// Purged; the ordinal is retired forever.
    Purged,
}

impl TenantState {
    fn to_byte(self) -> u8 {
        match self {
            Self::Active => 0,
            Self::Purged => 1,
        }
    }

    fn from_byte(byte: u8) -> Result<Self, StorageError> {
        match byte {
            0 => Ok(Self::Active),
            1 => Ok(Self::Purged),
            _ => Err(StorageError::CorruptLog),
        }
    }
}

/// Encode a scope → ordinal row value.
#[must_use]
pub fn encode_by_scope(tenant: TenantId, state: TenantState) -> Vec<u8> {
    let mut out = tenant.to_be_bytes().to_vec();
    out.push(state.to_byte());
    out
}

/// Decode a scope → ordinal row value.
///
/// # Errors
/// Returns [`StorageError::CorruptLog`] for a malformed value.
pub fn decode_by_scope(raw: &[u8]) -> Result<(TenantId, TenantState), StorageError> {
    let (ordinal, state) = raw.split_at_checked(4).ok_or(StorageError::CorruptLog)?;
    let ordinal: [u8; 4] = ordinal.try_into().map_err(|_| StorageError::CorruptLog)?;

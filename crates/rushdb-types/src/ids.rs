use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use uuid::Uuid;

/// Resolved tenant ordinal: the 4-byte prefix of every tenant-owned key
///. Assigned once by the tenant directory and never reused, so a purged
/// tenant's leftovers can never become visible to a later tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TenantId(pub u32);

impl TenantId {
    /// Reserved for system rows; never assigned to a tenant.
    pub const SYSTEM: Self = TenantId(0);
    /// The fixed tenant of single-tenant deployments.
    pub const SINGLE: Self = TenantId(1);
    /// First ordinal the tenant directory assigns.
    pub const FIRST_ASSIGNED: u32 = 2;
    /// A scope the directory has never registered. Never assigned, so it owns
    /// no keys: every read through it is empty.
    pub const UNREGISTERED: Self = TenantId(u32::MAX);

    /// The fixed identity used by single-tenant deployments.
    #[must_use]
    pub fn single() -> Self {
        Self::SINGLE
    }

    /// A tenant id from an ordinal the caller already resolved (tests,
    /// embedders, and the directory itself).
    ///
    /// # Panics
    /// Panics if `ordinal` does not fit the 4-byte key prefix.
    #[must_use]
    pub const fn from_ordinal(ordinal: u64) -> Self {
        assert!(ordinal <= u32::MAX as u64, "tenant ordinal exceeds u32");
        #[expect(clippy::cast_possible_truncation, reason = "range checked above")]
        TenantId(ordinal as u32)
    }

    /// Big-endian key-prefix bytes.
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 4] {

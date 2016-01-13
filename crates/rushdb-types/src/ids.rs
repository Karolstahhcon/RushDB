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
        self.0.to_be_bytes()
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unresolved tenant identity: the SHA-256 digest of the caller's scope
/// string. The tenant directory maps it to a [`TenantId`]; only the digest is
/// stored, never the scope string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TenantScope(pub [u8; 32]);

impl TenantScope {
    /// Digest of a scope value.
    #[must_use]
    pub fn of(scope: &str) -> Self {
        TenantScope(Sha256::digest(scope.as_bytes()).into())
    }

    /// The scope of single-tenant deployments.
    #[must_use]
    pub fn single() -> Self {
        Self::of("rushdb:single-tenant")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arbitrary_scope_values_are_stable_and_distinct() {
        assert_ne!(TenantScope::of("scope-a"), TenantScope::of("scope-b"));
        assert_eq!(TenantScope::of("not-a-uuid"), TenantScope::of("not-a-uuid"));
    }
}

macro_rules! dense_id {
    ($name:ident) => {
        #[doc = concat!("Dense monotonic ", stringify!($name), " identifier.")]
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
            Default,
        )]
        pub struct $name(#[doc = "Wrapped ordinal value."] pub u64);

        impl $name {
            /// Return the identifier that follows this one.
            #[must_use]
            pub fn next(self) -> Self {
                $name(self.0 + 1)
            }
        }
    };
}

dense_id!(RecordOrdinal);
dense_id!(RelationshipOrdinal);
dense_id!(OccurrenceOrdinal);
dense_id!(FamilyId);
dense_id!(PropertyId);
dense_id!(ValueSeq);
dense_id!(SpaceId);

/// Lexical token identifier.
pub type TokenId = u32;

/// Stable 128-bit public identity of a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordId(pub [u8; 16]);

impl RecordId {
    /// Generate a new time-ordered (`UUIDv7`) record id.
    #[must_use]

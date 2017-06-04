//! Fallible reconstruction for native pinned readers. Legacy reconstruction
//! remains only until its consumers migrate; do not adapt errors into nulls.
use crate::reader::{try_access as access, TryRead};
use rushdb_storage::StorageError;
use rushdb_types::encoding::decode_order_preserving;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::occurrence::{OccurrenceValue, PropertyFamily};
use rushdb_types::value::{ID_FIELD, LABEL_FIELD, SOURCE_FIELD, TARGET_FIELD, TYPE_FIELD};
use serde_json::{Map, Value as Json};
use std::collections::{BTreeMap, HashSet};

fn id_string(id: &[u8; 16]) -> String {
    uuid::Uuid::from_bytes(*id).simple().to_string()
}

/// Bounded family-definition cache for a single reconstruction session.
/// Within one pinned `ReadView`, property-family definitions are frozen, so a
/// session that reconstructs many records can resolve each distinct family
/// once instead of once per occurrence. The cache is capped (bounded resident
/// structure); beyond the cap, lookups fall back to direct reads. A session
/// must not outlive its view.
const MAX_CACHED_FAMILIES: usize = 4096;

/// Property families resolved by reconstruction sessions. A family and a
/// pid's family never change once written, so one cache serves every
/// session over views of the same tenant (a statement reconstructing many
/// records one at a time keeps it between sessions).
#[derive(Default)]
pub struct FamilyCache {
    family_cache: std::collections::HashMap<u64, Option<PropertyFamily>>,
    pid_family: std::collections::HashMap<u64, u64>,
}

/// Fallible reconstruction session over one pinned read view, with a bounded
/// property-family cache.
pub struct ReconstructSession<'a, R: TryRead> {
    snap: &'a R,
    tenant: TenantId,
    family_cache: std::collections::HashMap<u64, Option<PropertyFamily>>,
    pid_family: std::collections::HashMap<u64, u64>,
}

impl<'a, R: TryRead> ReconstructSession<'a, R> {
    /// Creates a session bound to `snap` for `tenant`.
    #[must_use]
    pub fn new(snap: &'a R, tenant: TenantId) -> Self {
        Self::with_cache(snap, tenant, FamilyCache::default())
    }

    /// A session that starts from (and can hand back) `cache`.
    #[must_use]
    pub fn with_cache(snap: &'a R, tenant: TenantId, cache: FamilyCache) -> Self {
        ReconstructSession {
            snap,
            tenant,
            family_cache: cache.family_cache,
            pid_family: cache.pid_family,
        }
    }

    /// The families this session resolved, for the next session.
    #[must_use]
    pub fn into_cache(self) -> FamilyCache {
        FamilyCache {
            family_cache: self.family_cache,
            pid_family: self.pid_family,
        }
    }

    /// Resolve a property family, caching the definition for the session.
    fn family(&mut self, fid: u64) -> Result<PropertyFamily, StorageError> {
        if let Some(cached) = self.family_cache.get(&fid) {
            return cached.clone().ok_or(StorageError::CorruptLog);
        }
        let value = access::family(self.snap, self.tenant, fid)?;
        if self.family_cache.len() < MAX_CACHED_FAMILIES {
            self.family_cache.insert(fid, value.clone());
        }
        value.ok_or(StorageError::CorruptLog)

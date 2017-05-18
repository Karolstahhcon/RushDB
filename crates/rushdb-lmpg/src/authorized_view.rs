//! Type-enforced policy gate for record/relationship reconstruction
//! (tenancy hardening, Pillar 4).
//!
//! The hard tenant boundary (`TenantId` as the mandatory keyspace prefix) is
//! structural: no access path can compile without a tenant to prefix a key
//! with. The finer-grained, same-tenant `DataAccessPolicy` (label grants,
//! property masks, relationship-type denials) was not — it was "evaluated by
//! callers," so a reconstruction call site could simply forget to apply it,
//! exactly the convention-based failure mode tenant isolation itself avoids.
//!
//! [`AuthorizedView`] closes that gap for the policy layer the same way: a
//! production read path that can return owner-identifying JSON constructs
//! one of these (always, even when the tenant has no policy configured) and
//! reconstructs through it, so the check cannot be omitted by a future call
//! site the way separate, caller-side masking could be.
use crate::reader::TryRead;
use crate::reconstruct;
use rushdb_storage::StorageError;
use rushdb_types::ids::TenantId;
use rushdb_types::security::{ActivePolicy, AuthorizationMode};
use rushdb_types::value::normalize_name;
use serde_json::{Map, Value as Json};

/// A read snapshot paired with the tenant's currently active (possibly
/// absent) data-access policy. `policy: None` — a tenant that has never
/// configured one — means no per-query filtering beyond the tenant boundary,
/// the same as an explicit [`AuthorizationMode::BoundaryOnly`] policy.
pub struct AuthorizedView<'a, R: TryRead> {
    snap: &'a R,
    tenant: TenantId,
    policy: Option<&'a ActivePolicy>,
}

impl<'a, R: TryRead> AuthorizedView<'a, R> {
    /// Binds `snap` to `tenant`'s `policy` (`None` when unconfigured).
    #[must_use]
    pub fn new(snap: &'a R, tenant: TenantId, policy: Option<&'a ActivePolicy>) -> Self {
        AuthorizedView {
            snap,
            tenant,
            policy,
        }
    }

    /// The tenant this view's policy was resolved for.
    #[must_use]

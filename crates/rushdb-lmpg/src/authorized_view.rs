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
    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// The smallest group a catalog read may report (POL-2 small-count
    /// suppression); 0 when there is no enforced policy or no threshold.
    #[must_use]
    pub fn min_count(&self) -> u64 {
        min_count(self.policy)
    }

    /// Whether `label` is in the authorized domain. Always `true` when the
    /// tenant has no policy, or its policy is `BoundaryOnly`.
    #[must_use]
    pub fn label_allowed(&self, label: &str) -> bool {
        label_allowed(self.policy, label)
    }

    /// Whether traversal through `rel_type` is authorized. Always `true`
    /// when the tenant has no policy, or its policy is `BoundaryOnly`.
    #[must_use]
    pub fn rel_type_allowed(&self, rel_type: &str) -> bool {
        rel_type_allowed(self.policy, rel_type)
    }

    /// Whether `property` is exposed for `label` — `label` itself must be
    /// allowed, and, when the label has a configured property mask, `name`
    /// must be one of its fields. Always `true` when the tenant has no
    /// policy, or its policy is `BoundaryOnly`.
    #[must_use]
    pub fn property_allowed(&self, label: &str, property: &str) -> bool {
        property_allowed(self.policy, label, property)
    }

    /// Gates `label` and, when allowed, retains only its authorized
    /// properties in `properties`. Returns `false` when `label` itself is
    /// denied, in which case the caller must drop the record rather than
    /// trust `properties`' contents.
    #[must_use]
    pub fn authorize_properties(&self, label: &str, properties: &mut Map<String, Json>) -> bool {
        if !self.label_allowed(label) {
            return false;
        }
        let Some(active) = self.policy else {
            return true;
        };

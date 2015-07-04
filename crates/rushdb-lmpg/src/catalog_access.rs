//! What a catalog read may name and count under the tenant's data-access
//! policy. Every catalog surface (`describe`, name suggestions, the
//! schema summary, value overlap, value search and SQL's tables) asks this
//! one type, so a new surface cannot apply the policy differently.
//!
//! The record-level checks are [`AuthorizedView`](crate::AuthorizedView)'s;
//! on top of them a catalog read withholds groups counted over fewer records
//! than the policy's `minCount`, and the extremes (min, max, samples) that
//! often are one record's value.

use std::sync::Arc;

use rushdb_types::security::{ActivePolicy, AuthorizationMode};

use crate::authorized_view;

/// The tenant's enforced policy, as catalog reads apply it. The default
/// (no enforced policy) shows everything.
#[derive(Clone, Debug, Default)]
pub struct CatalogAccess {
    policy: Option<Arc<ActivePolicy>>,
}

impl CatalogAccess {
    /// Applies `policy` when it is enforced; a `BoundaryOnly` policy (or
    /// none) restricts nothing.
    #[must_use]
    pub fn new(policy: Option<Arc<ActivePolicy>>) -> Self {
        Self {
            policy: policy.filter(|active| active.mode == AuthorizationMode::PolicyEnforced),
        }
    }

    /// Whether an enforced policy restricts what is shown.
    #[must_use]
    pub fn enforced(&self) -> bool {
        self.policy.is_some()
    }

    /// The enforced policy, if any.
    #[must_use]
    pub fn policy(&self) -> Option<&ActivePolicy> {
        self.policy.as_deref()
    }

    /// The smallest group a catalog read may report; 0 withholds nothing.
    #[must_use]
    pub fn min_count(&self) -> u64 {
        authorized_view::min_count(self.policy())
    }

    /// Whether `label` may be named.
    #[must_use]
    pub fn label(&self, label: &str) -> bool {
        authorized_view::label_allowed(self.policy(), label)
    }

    /// Whether `property` of `label` may be named.
    #[must_use]
    pub fn property(&self, label: &str, property: &str) -> bool {
        authorized_view::property_allowed(self.policy(), label, property)
    }

    /// Whether relationships of `rel_type` may be named.
    #[must_use]
    pub fn rel_type(&self, rel_type: &str) -> bool {
        authorized_view::rel_type_allowed(self.policy(), rel_type)
    }

    /// Whether a group counted over `records` records may be reported.
    #[must_use]
    pub fn counted(&self, records: u64) -> bool {
        records >= self.min_count()
    }

    /// Whether `label`, holding `records` records, may be listed.
    #[must_use]
    pub fn label_listed(&self, label: &str, records: u64) -> bool {

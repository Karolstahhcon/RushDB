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


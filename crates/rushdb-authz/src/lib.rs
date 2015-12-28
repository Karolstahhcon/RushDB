//! Graph-aware data access policies layered above the hard tenant boundary
//! (plan document 23). Policies are compiled once into a `SecurityScope` with
//! a stable fingerprint; enforcement is default-deny, evaluated by callers
//! over `MatchScope` candidates and mutation images, and can only narrow the
//! domain — never widen it.

use rushdb_lmpg::Lmpg;
use serde_json::Value as Json;
use thiserror::Error;

use rushdb_types::security::{policy_fingerprint, policy_fingerprint_with_min_count};
pub use rushdb_types::security::{ActivePolicy, AuthorizationMode, DataAccessPolicy};

type PolicyReader<'a> = dyn Fn(&[u8]) -> Result<Option<Vec<u8>>, rushdb_storage::StorageError> + 'a;

trait PolicyAccess {
    fn label_allowed(&self, mode: AuthorizationMode, label: &str) -> bool;
    fn rel_allowed(&self, rel_type: &str) -> bool;
}

impl PolicyAccess for DataAccessPolicy {
    fn label_allowed(&self, mode: AuthorizationMode, label: &str) -> bool {
        match mode {
            AuthorizationMode::BoundaryOnly => true,
            AuthorizationMode::PolicyEnforced => self.allows_label(label),
        }
    }

    fn rel_allowed(&self, rel_type: &str) -> bool {
        !self.denied_rel_types.iter().any(|t| t == rel_type)
    }
}

/// Compiled security scope bound to one policy generation.
#[derive(Clone)]
pub struct SecurityScope<'a> {
    /// Policy supplying the label grants, relationship denials, and masks.
    pub policy: &'a DataAccessPolicy,
    /// Mode controlling how strictly the policy is applied.
    pub mode: AuthorizationMode,
    fingerprint: u64,
}

impl<'a> SecurityScope<'a> {
    /// Compiles `policy` under `mode` and records its stable fingerprint.
    #[must_use]
    pub fn compile(policy: &'a DataAccessPolicy, mode: AuthorizationMode) -> Self {
        SecurityScope {
            policy,
            mode,
            fingerprint: policy_fingerprint(policy, mode),
        }
    }

    /// Returns the stable fingerprint identifying this policy generation.
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }
}

/// Errors produced while compiling or enforcing data access policies.
#[derive(Debug, Error)]
pub enum AuthzError {
    /// The storage layer failed while reading policy state.
    #[error("authorization storage error: {0}")]
    Storage(#[from] rushdb_storage::StorageError),
    /// Stored policy metadata is malformed or internally inconsistent.
    #[error("invalid authorization policy: {0}")]
    InvalidPolicy(String),
    /// The authorized query failed while executing against the engine.
    #[error("authorized query failed: {0}")]
    Query(#[from] rushdb_query::QueryError),
    /// The requested label lies outside the authorized domain.
    #[error("label `{0}` is outside the authorized domain")]
    LabelDenied(String),
    /// The requested relationship type is explicitly denied.
    #[error("relationship type `{0}` is denied")]
    RelDenied(String),
    /// No candidate matched the intersection of request and grants.
    #[error("policy denied all candidates for this request")]
    NoCandidates,
}

/// Mutation-side check over the new image (old-image revalidation happens
/// at commit inside the engine's own preconditions).
///
/// # Errors
///
/// Returns [`AuthzError::LabelDenied`] when the label is not granted in
/// [`AuthorizationMode::PolicyEnforced`] mode.
pub fn authorize_mutation(scope: &SecurityScope<'_>, label: &str) -> Result<(), AuthzError> {
    if matches!(scope.mode, AuthorizationMode::PolicyEnforced)
        && !scope.policy.label_allowed(scope.mode, label)
    {
        return Err(AuthzError::LabelDenied(label.to_string()));
    }
    Ok(())
}

/// Rejects traversal through a relationship type the policy denies.
///
/// # Errors

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
///
/// Returns [`AuthzError::RelDenied`] when the relationship type is denied.
pub fn authorize_traversal_type(
    scope: &SecurityScope<'_>,
    rel_type: &str,
) -> Result<(), AuthzError> {
    if scope.policy.rel_allowed(rel_type) {
        Ok(())
    } else {
        Err(AuthzError::RelDenied(rel_type.to_string()))
    }
}

fn mask_record_json(lmpg: &Lmpg, record: &mut Json, scope: &SecurityScope<'_>) {
    let Some(obj) = record.as_object_mut() else {
        return;
    };
    let raw_label = obj
        .get("__label")
        .and_then(|l| l.as_str())
        .unwrap_or("")
        .to_string();
    let label_norm = normalize(&raw_label);
    if matches!(scope.mode, AuthorizationMode::PolicyEnforced)
        && !scope.policy.allows_label(&raw_label)
    {
        obj.clear();
        return;
    }
    // enforce relationship-type grants through adjacency? traversal types
    // are checked eagerly by callers via authorize_traversal_type.
    let _ = lmpg;
    // masks are keyed by normalized label name (case-insensitive grants)
    let masked_fields = scope
        .policy
        .visible_properties
        .iter()
        .find(|(k, _)| normalize(k) == label_norm)
        .map(|(_, v)| v.clone());
    if let Some(masked) = masked_fields {
        obj.retain(|k, _| masked.iter().any(|m| normalize(k) == normalize(m)));
    }
}

fn normalize(name: &str) -> String {
    rushdb_types::value::normalize_name(name)
}

/// Authorized query execution (Records perspective): scans, masks output,
/// enforces result caps. Tenant scoping underneath is structural.
///
/// # Errors
///
/// Returns [`AuthzError::RelDenied`] or [`AuthzError::NoCandidates`] when the
/// request falls outside the authorized domain, and propagates
/// [`AuthzError::Query`] from the underlying query engine.
pub fn find_records_authorized(
    lmpg: &Lmpg,
    req: &rushdb_query::SearchRequest,
    scope: &SecurityScope<'_>,
) -> Result<rushdb_query::Page, AuthzError> {
    use rushdb_query::Page;

    // default-deny: explicit labels required in enforced mode
    let allowed_labels = match scope.mode {
        AuthorizationMode::BoundaryOnly => req.labels.clone(),
        AuthorizationMode::PolicyEnforced => {
            let requested: Vec<String> = if req.labels.is_empty() {
                // "all labels" request intersects the granted set: the labels
                // present that it allows, as stored (under `*`, every label:
                // left unnarrowed below).
                rushdb_lmpg::reader::try_access::labels(lmpg.store.as_ref(), lmpg.tenant)?
                    .into_iter()
                    .filter(|label| scope.policy.allows_label(label))
                    .collect()
            } else {
                req.labels
                    .iter()
                    .filter(|l| scope.policy.label_allowed(scope.mode, l))
                    .cloned()
                    .collect()
            };
            requested
        }
    };

    if let AuthorizationMode::PolicyEnforced = scope.mode {
        if let Some(spec) = req.related.first() {
            if let Some(rt) = &spec.rel_type {
                if !scope.policy.rel_allowed(rt) {
                    return Err(AuthzError::RelDenied(rt.clone()));
                }
            }
        }
    }

    // Under `*` an "all labels" request stays one.
    let every = matches!(scope.mode, AuthorizationMode::PolicyEnforced)
        && req.labels.is_empty()
        && scope.policy.allows_every_label();
    let allowed_labels = if every { Vec::new() } else { allowed_labels };
    let effective = rushdb_query::SearchRequest {
        labels: allowed_labels.clone(),

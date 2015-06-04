//! Integration tests for the default-deny authorization policy slice.

use std::sync::Arc;

use rushdb_authz::{authorize_mutation, AuthorizationMode, DataAccessPolicy, SecurityScope};
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_query::SearchRequest;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

fn policy_default_deny() -> DataAccessPolicy {
    DataAccessPolicy {
        allowed_labels: vec!["Public".into()],
        denied_rel_types: vec!["SECRET_LINK".into()],
        visible_properties: {

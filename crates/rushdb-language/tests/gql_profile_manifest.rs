//! The public GQL profile is machine-readable. Keep its advertised feature
//! identifiers aligned with the server capability and keep its evidence and
//! external-corpus boundary explicit.

use serde_json::Value;

const PROFILE: &str = include_str!("../conformance/gql-profile-v1.json");

#[test]
fn gql_profile_manifest_is_complete_and_traceable() {
    let profile: Value = serde_json::from_str(PROFILE).expect("valid GQL profile JSON");

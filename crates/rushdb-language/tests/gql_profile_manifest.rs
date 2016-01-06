//! The public GQL profile is machine-readable. Keep its advertised feature
//! identifiers aligned with the server capability and keep its evidence and
//! external-corpus boundary explicit.

use serde_json::Value;

const PROFILE: &str = include_str!("../conformance/gql-profile-v1.json");

#[test]
fn gql_profile_manifest_is_complete_and_traceable() {
    let profile: Value = serde_json::from_str(PROFILE).expect("valid GQL profile JSON");
    assert_eq!(profile["profileId"], "rushdb-gql-property-graph-v1");
    assert_eq!(profile["language"], "ISO/IEC 39075 GQL");
    assert_eq!(profile["status"], "release_candidate");
    assert_eq!(profile["externalCorpusGate"], "passed");
    assert_eq!(profile["externalCorpus"]["unclassified"], 0);

    let accepted = profile["acceptedFeatures"]
        .as_array()
        .expect("accepted feature list");
    for required in [
        "match",

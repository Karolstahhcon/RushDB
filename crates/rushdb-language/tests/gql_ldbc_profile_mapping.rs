//! Every reference query of the pinned LDBC SNB Interactive v1 Cypher
//! implementation is classified for the published GQL profile
//! (`conformance/gql-ldbc-snb-v1-mapping.json`): accepted cases run against
//! the manifest's fixture and must return their hand-derived rows (reads) or
//! commit their effect (writes); the rest name why they are not mapped.

use std::collections::BTreeSet;
use std::sync::Arc;

use rushdb_language::{execute, execute_mutation, QueryLanguage};
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::Value;

const MAPPING: &str = include_str!("../conformance/gql-ldbc-snb-v1-mapping.json");

fn fixture(mapping: &Value) -> Lmpg {
    let graph = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::from_ordinal(1));
    execute_mutation(
        &graph,
        QueryLanguage::Gql,
        mapping["fixture"].as_str().expect("fixture script"),
        None,
    )
    .expect("fixture loads");
    graph
}

#[test]
fn gql_profile_classifies_and_executes_the_ldbc_corpus() {

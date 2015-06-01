//! The Gremlin traversal profile (`conformance/gremlin-profile-v1.json`):
//! every traversal runs on the engine with the expected row count or
//! effect, and every bytecode case translates to its pinned traversal. CI
//! (`gremlin-grammar`) checks the same traversals against the official
//! `TinkerPop` grammar.

use std::sync::Arc;

use rushdb_language::{execute, execute_mutation, gremlin_bytecode, QueryLanguage};
use rushdb_lmpg::{plan::FieldChange, Lmpg, RecordRef};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

const PROFILE: &str = include_str!("../conformance/gremlin-profile-v1.json");

/// The profile's fixture graph.
fn fixture() -> Lmpg {
    let graph = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::from_ordinal(9));
    let person = |name: &str, age: i64| -> RecordRef {
        graph
            .create_record(
                "Person",
                None,
                vec![
                    ("name".into(), FieldChange::Set(json!(name))),
                    ("age".into(), FieldChange::Set(json!(age))),
                ],
            )
            .unwrap()
    };
    let alice = person("alice", 29);
    let bob = person("bob", 27);
    let carol = person("carol", 32);
    let dave = person("dave", 35);
    let lop = graph
        .create_record(
            "Software",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!("lop"))),
                ("lang".into(), FieldChange::Set(json!("java"))),
            ],
        )
        .unwrap();
    for (rel, a, b) in [
        ("KNOWS", alice, bob),
        ("KNOWS", alice, carol),
        ("KNOWS", carol, dave),
        ("CREATED", alice, lop),
        ("CREATED", carol, lop),
    ] {
        graph.create_relationship(rel, a, b, vec![]).unwrap();
    }
    graph
}

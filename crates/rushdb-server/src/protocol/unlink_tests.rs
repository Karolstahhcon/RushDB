//! `unlinkRecordsById` without types or a direction deletes every
//! relationship between the records, whatever its type and whichever way it
//! points (what detach meant in `RushDB` 2.x).

use super::transaction_reference_tests::run;
use super::*;

fn person(node: &Node, name: &str) -> String {
    let created = node
        .scope()
        .create_record(
            "Person",
            &serde_json::json!({ "name": name }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    created["id"].as_str().unwrap().to_owned()
}

fn mutate(node: &Node, kind: &str, command: &serde_json::Value) -> serde_json::Value {
    run(
        node,
        &serde_json::json!({"kind": "mutate", "mutation": {"kind": kind, "command": command}}),
    )
    .unwrap()
}

fn link(node: &Node, relation_type: &str, source: &str, target: &str) {
    mutate(
        node,

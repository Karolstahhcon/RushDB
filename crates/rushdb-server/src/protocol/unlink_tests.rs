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
        "linkRecordsById",
        &serde_json::json!({"relationType": relation_type, "sourceIds": [source], "targetIds": [target]}),
    );
}

#[test]
fn untyped_undirected_unlink_deletes_every_relationship_between_the_records() {
    let node = Node::bootstrap(&mut super::tests::config(false)).unwrap();
    let ada = person(&node, "Ada");
    let bob = person(&node, "Bob");
    let eve = person(&node, "Eve");
    link(&node, "KNOWS", &ada, &bob);
    link(&node, "ADVISES", &ada, &bob);
    link(&node, "LIKES", &bob, &ada);
    link(&node, "KNOWS", &ada, &eve);

    let typed = mutate(
        &node,
        "unlinkRecordsById",
        &serde_json::json!({"relationTypes": ["ADVISES", "LIKES"], "direction": "out",
            "sourceIds": [ada], "targetIds": [bob]}),
    );
    assert_eq!(typed["affected"], 1, "only ADVISES points out from Ada");

    let everything = serde_json::json!({"sourceIds": [ada], "targetIds": [bob]});
    assert_eq!(
        mutate(&node, "unlinkRecordsById", &everything)["affected"],
        2
    );
    assert_eq!(
        mutate(&node, "unlinkRecordsById", &everything)["affected"],

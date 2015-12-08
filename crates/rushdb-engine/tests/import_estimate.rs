//! Regression coverage for context admission of nested imports.

use rushdb_engine::{Engine, OperationClass};
use rushdb_query::SearchRequest;
use serde_json::json;

fn ten_level_nested_document() -> serde_json::Value {
    let mut document = json!({"v": 1});
    for _ in 0..10 {
        document = json!({"child": document});
    }
    document
}

#[test]
fn nested_import_still_rejects_a_tiny_budget() {
    let engine = Engine::new_in_memory();
    let document = ten_level_nested_document();
    let mut context = engine
        .context(None, OperationClass::Write, None, Some(1))

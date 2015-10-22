//! Regression coverage for context admission of nested imports.

use rushdb_engine::{Engine, OperationClass};
use rushdb_query::SearchRequest;
use serde_json::json;

fn ten_level_nested_document() -> serde_json::Value {
    let mut document = json!({"v": 1});
    for _ in 0..10 {
        document = json!({"child": document});

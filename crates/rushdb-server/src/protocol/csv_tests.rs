use rushdb_protocol::CsvParseConfig;
use serde_json::{json, Value};

use super::{decode, render};

/// Papa Parse 5.4 and the platform's clean-up on the same inputs; see
/// `testdata/papaparse-csv-oracle.cjs`.
const ORACLE: &str = include_str!("testdata/papaparse-csv-oracle.json");

fn oracle() -> serde_json::Map<String, Value> {
    serde_json::from_str::<Value>(ORACLE)
        .expect("oracle is JSON")
        .as_object()
        .expect("oracle is an object")
        .clone()
}

#[test]
fn decoding_reproduces_the_platform_import_case_by_case() {
    let mut checked = 0;
    for (name, case) in oracle() {
        if name == "__export" {
            continue;
        }
        let input = &case["input"];
        let data = input["data"].as_str().expect("data");
        let config: CsvParseConfig =
            serde_json::from_value(input["parseConfig"].clone()).expect("parse config");
        let suggest_types = input["options"]["suggestTypes"].as_bool().unwrap_or(false);
        let decoded = decode(data, &config, suggest_types);
        match (case.get("error"), decoded) {
            (Some(expected), Err(actual)) => assert_eq!(
                expected.as_str().expect("error text"),
                format!("CSV parse error: {actual}"),
                "case {name}"
            ),
            (None, Ok(rows)) => assert_eq!(case["rows"], Value::Array(rows), "case {name}"),
            (expected, actual) => panic!("case {name}: expected {expected:?}, got {actual:?}"),
        }
        checked += 1;
    }
    assert!(checked >= 59, "only {checked} oracle cases ran");
}

#[test]
fn export_reproduces_the_platform_export() {
    let oracle = oracle();
    let export = &oracle["__export"];

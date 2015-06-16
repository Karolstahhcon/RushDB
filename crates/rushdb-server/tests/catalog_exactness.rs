//! Catalog facts an agent reads as exact are exact (LMPG catalog audit,
//! 2026-10-24): describe ranges cover every record, not the first 10k;
//! type splits are reported on the label that has them; a name stored as
//! text on one label does not hide another label's numeric range; counts
//! and top-k over long texts fit the working set; and a name spelled
//! exactly ranks first among equal suggestions.

use std::sync::Arc;

use rushdb_protocol::{EnvelopeHandler, RequestEnvelope};
use rushdb_server::{Node, NodeEnvelopeHandler, ServerConfig};
use serde_json::{json, Value};

fn node(query_memory_max: usize) -> NodeEnvelopeHandler {
    let resources = rushdb_server::resource_config::ResourceConfig {
        query_memory_max: rushdb_server::resource_config::ByteSize(query_memory_max),
        ..Default::default()
    };
    let mut cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "t".into(),
        data_dir: None,
        multitenant: false,
        capability_secret: None,
        raft: None,
        resources,
        transactions: rushdb_server::resource_config::TransactionConfig::default(),
        languages: rushdb_protocol::LanguageSet::default(),
        max_request_bytes: 64 * 1024 * 1024,
        listen_addr: "127.0.0.1".into(),
        tls_cert: None,
        tls_key: None,
        maintenance: false,
        allow_insecure_listen: false,
        require_index_warmup: false,
        request_timeout_ms: 0,
        max_concurrent_requests_per_tenant: 0,
        explicit_flags: std::collections::BTreeSet::new(),
    };
    NodeEnvelopeHandler::new(Arc::new(Node::bootstrap(&mut cfg).expect("bootstrap")))
}

fn call(handler: &NodeEnvelopeHandler, operation: &Value) -> Value {
    let envelope: RequestEnvelope =
        serde_json::from_value(json!({ "protocolVersion": 1, "operation": operation }))
            .expect("envelope");
    let response = handler.execute(envelope, None).expect("request succeeds");
    serde_json::to_value(response.response).expect("response json")
}

fn ingest(handler: &NodeEnvelopeHandler, label: &str, documents: &[Value]) {
    for chunk in documents.chunks(2000) {
        call(
            handler,
            &json!({ "kind": "ingest", "ingest": {
                "kind": "documents", "label": label, "documents": chunk,
                "options": { "suggestTypes": true } } }),
        );
    }
}

fn properties(handler: &NodeEnvelopeHandler, label: &str) -> Vec<Value> {
    let out = call(
        handler,
        &json!({ "kind": "describe", "describe": {
            "kind": "properties", "query": { "labels": [label] } } }),
    );
    out["properties"].as_array().cloned().unwrap_or_default()
}

fn property<'a>(properties: &'a [Value], name: &str) -> &'a Value {
    properties
        .iter()
        .find(|p| p["name"] == name)
        .unwrap_or_else(|| panic!("no property {name} in {properties:?}"))
}

#[test]
fn describe_ranges_cover_every_record_not_a_sample() {
    let handler = node(256 * 1024 * 1024);
    // The extremes arrive after the first 10,000 records.
    let mut documents: Vec<Value> = (0..12_000)
        .map(|i| json!({ "zip": 1000 + (i % 500), "at": "2020-01-02" }))
        .collect();
    documents.push(json!({ "zip": 99_950, "at": "2031-05-06T07:08:09Z" }));
    documents.push(json!({ "zip": 501, "at": "1999-12-31" }));
    ingest(&handler, "zip_code", &documents);
    let properties = properties(&handler, "zip_code");
    let zip = property(&properties, "zip");
    assert_eq!(
        (zip["min"].clone(), zip["max"].clone()),
        (json!(501), json!(99_950))
    );
    let at = property(&properties, "at");
    assert_eq!(at["min"], json!("1999-12-31"));
    assert_eq!(at["max"], json!("2031-05-06T07:08:09.000Z"));
}

#[test]
fn type_splits_and_ranges_are_per_label() {
    let handler = node(256 * 1024 * 1024);
    // `Date` is text on one label and a datetime on another; `mcmId` a
    // number on one label and text on another.
    ingest(
        &handler,
        "yearmonth",
        &[
            json!({ "Date": "201207", "mcmId": "16165" }),
            json!({ "Date": "201302", "mcmId": "7" }),
        ],
    );
    ingest(
        &handler,
        "transactions",
        &[
            json!({ "Date": "2012-08-24", "mcmId": 4 }),
            json!({ "Date": "2012-08-26", "mcmId": 3660 }),
        ],
    );
    let yearmonth = properties(&handler, "yearmonth");
    let transactions = properties(&handler, "transactions");
    for (properties, name) in [
        (&yearmonth, "Date"),
        (&yearmonth, "mcmId"),
        (&transactions, "Date"),
        (&transactions, "mcmId"),
    ] {
        let p = property(properties, name);
        assert!(
            p.get("siblingTypes")
                .is_none_or(|s| s.as_array().is_some_and(Vec::is_empty)),
            "{name}: no type split on this label: {p}"
        );
    }
    let summary = call(
        &handler,
        &json!({ "kind": "describe", "describe": { "kind": "schemaSummary", "fewValues": 10 } }),
    );
    let label = summary["labels"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["name"] == "transactions")
        .unwrap();
    let mcm = label["properties"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "mcmId")
        .unwrap();
    assert_eq!(
        (mcm["min"].clone(), mcm["max"].clone()),
        (json!(4), json!(3660)),
        "{mcm}"
    );
}

#[test]
fn counts_and_top_values_of_long_texts_fit_the_working_set() {
    // 2,000 distinct texts of ~400 bytes need ~1.6 MB held as values; the
    // working set allows 512 KiB.
    let handler = node(512 * 1024);
    let documents: Vec<Value> = (0..2_000)
        .map(|i| json!({ "Text": format!("{i:05} {}", "lorem ipsum ".repeat(33)) }))
        .chain((0..5).map(|_| json!({ "Text": format!("common {}", "x".repeat(400)) })))
        .collect();
    ingest(&handler, "postHistory", &documents);
    let values = |mode: &str| {
        call(
            &handler,
            &json!({ "kind": "query", "query": {
                "kind": "propertyValues", "property": "Text",
                "query": { "labels": ["postHistory"] }, "mode": mode, "limit": 3 } }),
        )
    };
    assert_eq!(values("count")["distinctCount"], json!(2_001));
    let top = values("topK");
    assert_eq!(top["distinctCount"], json!(2_001));
    assert_eq!(top["counts"], json!([5, 1, 1]));
    assert_eq!(
        top["values"][0],
        json!(format!("common {}", "x".repeat(400)))
    );
    assert!(top["values"][1].as_str().unwrap().starts_with("00000 "));
}

#[test]
fn a_name_spelled_exactly_ranks_first_among_equal_scores() {
    let handler = node(256 * 1024 * 1024);
    ingest(
        &handler,
        "cards",
        &[json!({ "type": "Creature", "types": "Creature" })],
    );
    ingest(&handler, "tags", &[json!({ "TagName": "bayes" })]);
    ingest(&handler, "posts", &[json!({ "Tags": "<bayes>" })]);
    let names = |text: &str| {
        call(
            &handler,
            &json!({ "kind": "describe", "describe": { "kind": "names", "text": text, "limit": 3 } }),
        )["names"]
            .clone()
    };
    let first = names("cards.types");
    assert_eq!(
        (first[0]["label"].clone(), first[0]["property"].clone()),
        (json!("cards"), json!("types"))
    );
    let first = names("tags");
    assert_eq!(first[0]["label"], json!("tags"));
    assert!(
        first[0].get("property").is_none_or(Value::is_null),
        "{first}"
    );
}

fn matching(handler: &NodeEnvelopeHandler, label: &str, predicate: &Value) -> usize {
    let out = call(
        handler,
        &json!({ "kind": "query", "query": { "kind": "records", "query": {
            "labels": [label], "predicate": predicate, "limit": 1000 } } }),
    );
    out["records"].as_array().map_or(0, Vec::len)
}

fn values(handler: &NodeEnvelopeHandler, label: &str, property: &str, mode: &str) -> Value {
    call(
        handler,
        &json!({ "kind": "query", "query": { "kind": "propertyValues", "property": property,
            "query": { "labels": [label] }, "mode": mode, "limit": 100 } }),
    )
}

#[test]
fn integers_after_a_float_are_filtered_and_described_as_numbers() {
    let handler = node(256 * 1024 * 1024);
    // A float first, then integers (`hyperdrive_rating` in the LMPG JSON
    // audit: `$gte 1` matched 5 of 34 records and the range read 5e-324).
    ingest(
        &handler,
        "floatFirst",
        &[
            json!({ "x": 0.5 }),
            json!({ "x": 1 }),
            json!({ "x": 2 }),
            json!({ "x": 1.5 }),
            json!({ "x": 4 }),
        ],
    );
    // Integers first, then a float.
    ingest(
        &handler,
        "intFirst",
        &[json!({ "x": 1 }), json!({ "x": 1.1 }), json!({ "x": 3 })],
    );
    // Mixed number arrays.
    ingest(
        &handler,
        "arrays",
        &[json!({ "x": [1, 2.5] }), json!({ "x": [0.5, 3] })],
    );
    for (label, at_least_one, min, max) in [
        ("floatFirst", 4, json!(0.5), json!(4)),
        ("intFirst", 3, json!(1), json!(3)),
    ] {
        assert_eq!(
            matching(&handler, label, &json!({ "x": { "$gte": 1 } })),
            at_least_one,
            "{label}"
        );
        assert_eq!(
            matching(&handler, label, &json!({ "x": { "$lt": 1.25 } })),
            2, // 0.5 and 1; 1 and 1.1
            "{label}"
        );
        let range = values(&handler, label, "x", "range");
        assert_eq!(
            (range["min"].clone(), range["max"].clone()),
            (min, max),
            "{label}: {range}"
        );
        let p = property(&properties(&handler, label), "x").clone();
        assert_eq!(p["type"], json!("number"), "{label}: {p}");
        let top = values(&handler, label, "x", "topK");
        assert!(
            top["values"]
                .as_array()
                .unwrap()
                .iter()
                .all(|v| v.as_f64().is_some_and(|f| f >= 0.5)),
            "{label}: {top}"
        );
    }
    assert_eq!(
        matching(&handler, "arrays", &json!({ "x": { "$gte": 3 } })),
        1
    );
    assert_eq!(
        matching(&handler, "arrays", &json!({ "x": { "$lt": 1 } })),
        1
    );
}

#[test]
fn nulls_are_counted_once_as_the_null_entry_and_never_as_values() {
    let handler = node(256 * 1024 * 1024);
    // Nulls before and after the first value (LMPG JSON audit J1, J5:
    // `founder_character_id` read 31 of 34; `destroyed_at` string 22 /
    // datetime 17 for 2 strings, 12 datetimes and 25 nulls).
    ingest(
        &handler,
        "faction",
        &[
            json!({ "founder": null, "at": null }),
            json!({ "founder": null, "at": "2004-01-01T18:00:00Z" }),
            json!({ "founder": "palpatine", "at": null }),
            json!({ "founder": null, "at": "a long time ago" }),
            json!({ "founder": "dooku", "at": null }),
            json!({ "founder": "palpatine" }),
        ],
    );
    let described = properties(&handler, "faction");
    let entries = |name: &str| -> Vec<(String, u64)> {
        let mut out: Vec<(String, u64)> = described
            .iter()
            .filter(|p| p["name"] == name)
            .map(|p| {
                (
                    p["type"].as_str().unwrap().to_string(),
                    p["recordsCount"].as_u64().unwrap(),
                )
            })
            .collect();
        out.sort();
        out
    };

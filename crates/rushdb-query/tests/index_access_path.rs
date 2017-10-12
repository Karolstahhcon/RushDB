//! Equality/In index access path: the maintained postings must return the
//! exact same match sets as the full-scan fallback, including numeric
//! int/float union, arrays, missing fields, label scoping, and shapes the
//! index declines (`$ne`, `$or`).

use std::collections::BTreeSet;
use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_query::document::parse_document;
use rushdb_query::{find_records, find_records_streaming};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TenantId::from_ordinal(77))
}

fn seed(lmpg: &Lmpg) {
    let set = |fields: Vec<(&str, Value)>| -> Vec<(String, FieldChange)> {
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), FieldChange::Set(v)))
            .collect()
    };
    // string / int / float / bool / array / missing
    lmpg.create_record(
        "A",
        None,
        set(vec![
            ("s", json!("alpha")),
            ("n", json!(7)),
            ("flag", json!(true)),
            ("tags", json!(["red", "blue"])),
        ]),
    )
    .unwrap();
    lmpg.create_record(
        "A",
        None,
        set(vec![("s", json!("beta")), ("n", json!(7.0))]),
    )

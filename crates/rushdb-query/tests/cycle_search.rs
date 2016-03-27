//! `$cycle`: a record lies on a closed trail (no relationship used twice) of
//! `min..=max` hops. Answered per query from one graph and a shortest-cycle
//! search per record, so long rings in branching graphs return promptly; a
//! longer trail than the shortest cycle is searched within a step budget.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{Lmpg, RecordRef};
use rushdb_query::document::parse_document;
use rushdb_query::{find_records, QueryError};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

fn setup() -> Lmpg {
    Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::from_ordinal(91))
}

fn record(lmpg: &Lmpg, label: &str, name: &str) -> RecordRef {
    lmpg.create_record(
        label,
        None,
        vec![("name".into(), FieldChange::Set(json!(name)))],
    )
    .unwrap()
}

fn link(lmpg: &Lmpg, from: RecordRef, to: RecordRef) {
    lmpg.create_relationship("FLOW", from, to, vec![]).unwrap();
}

fn cycle_query(label: &str, direction: &str, min: u64, max: u64) -> Value {
    json!({ "labels": [label], "where": {
        "$cycle": { "type": "FLOW", "direction": direction, "hops": { "min": min, "max": max } }
    }, "limit": 1000 })
}

fn matches(lmpg: &Lmpg, document: &Value) -> Result<Vec<String>, QueryError> {
    let search = parse_document(document).expect("valid query").search;
    let page = find_records(lmpg, &search)?;

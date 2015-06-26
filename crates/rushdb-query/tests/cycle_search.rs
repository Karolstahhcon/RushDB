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
    let mut names: Vec<String> = page
        .records
        .iter()
        .filter_map(|r| r["name"].as_str().map(str::to_owned))
        .collect();
    names.sort();
    Ok(names)
}

#[test]
fn a_long_ring_inside_branching_traffic_is_found_promptly() {
    let lmpg = setup();
    // A 20-hop ring...
    let ring: Vec<RecordRef> = (0..20)
        .map(|i| record(&lmpg, "TX", &format!("ring-{i:02}")))
        .collect();
    for i in 0..20 {
        link(&lmpg, ring[i], ring[(i + 1) % 20]);
    }
    // ...among 300 records with three pseudo-random successors each, the
    // shape that made per-record trail enumeration exponential.
    let noise: Vec<RecordRef> = (0..300)
        .map(|i| record(&lmpg, "TX", &format!("noise-{i:03}")))
        .collect();
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    for &from in &noise {
        for _ in 0..3 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            link(&lmpg, from, noise[usize::try_from(seed % 300).unwrap()]);
        }
    }

    let started = Instant::now();
    let found = matches(&lmpg, &cycle_query("TX", "out", 2, 20)).unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "took {:?}",
        started.elapsed()
    );
    for i in 0..20 {
        assert!(

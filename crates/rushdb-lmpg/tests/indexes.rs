//! Declared ordered indexes: entries are derived rows that always equal
//! a from-scratch derivation (the scrub oracle), during a build, under
//! concurrent writers, and through a drop.

use std::sync::Arc;

use rushdb_lmpg::index::{self, IndexStep};
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{scrub, Lmpg};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use rushdb_types::index::{IndexKind, IndexState};
use serde_json::json;

const TENANT: TenantId = TenantId::from_ordinal(4040);

fn engine() -> Lmpg {
    Lmpg::new(Arc::new(Store::new_in_memory()), TENANT)
}

fn assert_oracle(eng: &Lmpg, context: &str) {
    let view = eng.store.read_view().unwrap();
    let violations = scrub::scrub(&view, eng.tenant).unwrap();
    assert!(violations.is_empty(), "{context}: {violations:?}");
}

fn entry_count(eng: &Lmpg, id: u32) -> usize {
    let view = eng.store.read_view().unwrap();
    index::entries(&view, eng.tenant, id, &[], &[])
        .unwrap()
        .len()
}

fn build(eng: &Lmpg, id: u32, batch: usize) {
    for _ in 0..10_000 {
        match eng.index_step(id, batch).unwrap() {
            IndexStep::Finished | IndexStep::Idle => return,
            IndexStep::Advanced(_) => {}
        }
    }
    panic!("build did not finish");
}

#[test]
fn building_index_covers_existing_records_and_tracks_edits() {
    let eng = engine();
    let mut records = Vec::new();
    for i in 0..25 {
        records.push(
            eng.create_record(
                "Item",
                None,
                vec![("price".into(), FieldChange::Set(json!(i % 7)))],
            )
            .unwrap(),
        );
    }
    eng.create_record(
        "Other",
        None,
        vec![("price".into(), FieldChange::Set(json!(1)))],
    )
    .unwrap();
    let def = eng
        .define_index("item_price", "Item", "price", IndexKind::Ordered)
        .unwrap();
    assert_eq!(def.state, IndexState::Building { cursor: 0 });
    assert_oracle(&eng, "defined");
    // Step in small batches, editing covered and uncovered records between
    // steps: the oracle holds after every commit.
    let mut steps = 0;
    loop {

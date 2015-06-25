//! Layout v3: derived rows (presence, value dictionary, counters, array
//! markers) are computed at apply time from base rows (DC11/DC12). The
//! oracle is `scrub::derive_from_scratch`: after every commit the stored
//! derived rows must equal the rows derived from the base rows alone.

use std::sync::Arc;

use rushdb_lmpg::derived::{self, AdjDirection};
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::{scrub, Lmpg, RecordRef, RelationshipRef};
use rushdb_storage::{Op, OverlayView, Store};
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys as k;
use serde_json::json;

const TENANT: TenantId = TenantId::from_ordinal(1234);

fn engine() -> Lmpg {
    Lmpg::new(Arc::new(Store::new_in_memory()), TENANT)
}

/// Stored derived rows equal the rows derived from scratch, and the base
/// cross-checks (headers, label membership, adjacency) hold.
fn assert_oracle(eng: &Lmpg, context: &str) {
    let view = eng.store.read_view().unwrap();
    let report = scrub::scrub_report(&view, eng.tenant).unwrap();
    assert!(report.complete, "{context}");
    assert!(
        report.violations.is_empty(),
        "{context}: {:?}",
        report.violations
    );
    for label in try_access::labels(&view, eng.tenant).unwrap() {
        assert_eq!(
            derived::label_count(&view, eng.tenant, &label).unwrap(),
            try_access::label_members(&view, eng.tenant, &label)
                .unwrap()
                .len() as u64,
            "{context}: label {label}"
        );
    }
}

fn pid_of(eng: &Lmpg, name: &str) -> u64 {
    let view = eng.store.read_view().unwrap();
    let mut prefix = k::tenant_prefix(eng.tenant).to_vec();
    prefix.push(k::F_OCC);
    for (key, _) in view.range_prefix(&prefix).unwrap() {
        let (_, kind, _, pid, _) = k::parse_occ_key(&key).unwrap();
        let family = try_access::definition(&view, eng.tenant, pid)
            .unwrap()
            .and_then(|d| try_access::family(&view, eng.tenant, d.family_id).unwrap());
        if kind == OwnerKind::Record && family.is_some_and(|f| f.normalized_name == name) {
            return pid;
        }
    }
    panic!("no occurrence of {name}");
}

/// Deterministic xorshift so failures replay.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % n as u64).unwrap()
    }
}

fn random_value(rng: &mut Rng) -> FieldChange {
    match rng.below(7) {
        0 => FieldChange::ExplicitNull,
        1 => FieldChange::Set(json!([])),
        2 => FieldChange::Set(json!(["a", "b", rng.below(3)])),
        // Over DICT_MAX_VALUE_BYTES: excluded from the dictionary.
        3 => FieldChange::Set(json!("x".repeat(120 + rng.below(4)))),
        4 => FieldChange::Set(json!(format!("s{}", rng.below(5)))),
        5 => FieldChange::Set(json!(f64::from(u8::try_from(rng.below(4)).unwrap()) + 0.5)),
        _ => FieldChange::Set(json!(rng.below(6))),
    }
}

#[test]
fn derived_rows_equal_the_scratch_derivation_after_every_commit() {
    let eng = engine();
    let labels = ["A", "B", "C"];
    let fields = ["n", "s", "tags"];
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut records: Vec<(RecordRef, &str)> = Vec::new();
    let mut rels: Vec<RelationshipRef> = Vec::new();
    let mut ordered: Option<u32> = None;
    for step in 0..300 {
        // Declare an index mid-workload and backfill it in small
        // steps interleaved with every kind of edit.
        if step == 40 {
            ordered = Some(
                eng.define_index("a_n", "A", "n", rushdb_types::index::IndexKind::Ordered)
                    .unwrap()
                    .id,
            );
        }
        if let Some(id) = ordered.filter(|_| step % 7 == 0) {
            eng.index_step(id, 3).unwrap();
        }
        if step == 250 {
            eng.drop_index("a_n").unwrap();
        }
        let op = if records.len() < 4 { 0 } else { rng.below(8) };
        match op {
            0 | 1 => {
                let label = labels[rng.below(labels.len())];
                let mut set = Vec::new();
                for field in fields {
                    if rng.below(3) > 0 {
                        set.push((field.to_string(), random_value(&mut rng)));
                    }
                }
                records.push((eng.create_record(label, None, set).unwrap(), label));
            }
            2 | 3 => {
                let (record, _) = records[rng.below(records.len())];
                let field = fields[rng.below(fields.len())];
                let change = if rng.below(5) == 0 {
                    FieldChange::Delete
                } else {
                    random_value(&mut rng)
                };
                eng.update_fields(record, vec![(field.into(), change)])
                    .unwrap();
            }
            4 => {
                let at = rng.below(records.len());
                let (record, from) = records[at];
                let to = labels[(labels.iter().position(|l| *l == from).unwrap() + 1) % 3];
                eng.retype_record(record, from, to).unwrap();
                records[at].1 = to;
            }
            5 => {
                let (record, _) = records.swap_remove(rng.below(records.len()));
                eng.delete_record(record).unwrap();
                // Cascade removed this record's relationships.
                let view = eng.store.read_view().unwrap();
                rels.retain(|r| {
                    try_access::relationship_by_id(
                        &view,
                        eng.tenant,
                        &rushdb_types::ids::RelationshipId(r.relationship_id),
                    )
                    .unwrap()
                    .is_some()
                });
            }
            6 => {
                let (source, _) = records[rng.below(records.len())];
                let (target, _) = records[rng.below(records.len())];
                let rel_type = ["KNOWS", "LIKES"][rng.below(2)];
                rels.push(

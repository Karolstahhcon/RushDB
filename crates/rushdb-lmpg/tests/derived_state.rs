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
                    eng.create_relationship(
                        rel_type,
                        source,
                        target,
                        vec![("w".into(), random_value(&mut rng))],
                    )
                    .unwrap(),
                );
            }
            _ => {
                if !rels.is_empty() {
                    let rel = rels.swap_remove(rng.below(rels.len()));
                    eng.delete_relationship(rel).unwrap();
                }
            }
        }
        assert_oracle(&eng, &format!("step {step} op {op}"));
    }
}

#[test]
fn pinned_view_sees_the_historical_derived_count() {
    let eng = engine();
    for i in 0..10 {
        eng.create_record("BOOK", None, vec![("n".into(), FieldChange::Set(json!(i)))])
            .unwrap();
    }
    let early = eng.store.read_at(eng.store.committed_version()).unwrap();
    for i in 10..30 {
        eng.create_record("BOOK", None, vec![("n".into(), FieldChange::Set(json!(i)))])
            .unwrap();
    }
    let latest = eng.store.read_view().unwrap();
    assert_eq!(
        derived::label_count(&latest, eng.tenant, "BOOK").unwrap(),
        30
    );
    // Derived rows are MVCC rows written at the base commit's index, so a
    // pinned view answers exactly for its snapshot.
    assert_eq!(
        derived::label_count(&early, eng.tenant, "BOOK").unwrap(),
        10
    );
    let pid = pid_of(&eng, "n");
    assert_eq!(
        derived::prop_support_total(&early, eng.tenant, OwnerKind::Record, pid).unwrap(),
        10
    );
    assert_eq!(
        derived::dictionary(&early, eng.tenant, OwnerKind::Record, pid, &[])
            .unwrap()
            .len(),
        10
    );
}

#[test]
fn transaction_overlay_derives_its_staged_writes() {
    let eng = engine();
    eng.create_record("BOOK", None, vec![]).unwrap();
    let base = Arc::new(eng.store.read_view().unwrap());
    let mut overlay = OverlayView::new(Arc::clone(&base));
    assert_eq!(
        derived::label_count(&overlay, eng.tenant, "BOOK").unwrap(),
        1
    );
    // Stage a base header + membership for a new ordinal; reading a derived
    // key through the overlay derives the staged writes.
    overlay.apply_ops(&[
        Op::Put(
            k::record_hdr_key(eng.tenant, 999),
            k::encode_record_hdr(&[7; 16], "BOOK"),
        ),
        Op::Put(k::label_member_key(eng.tenant, "BOOK", 999), Vec::new()),
    ]);
    assert_eq!(
        derived::label_count(&overlay, eng.tenant, "BOOK").unwrap(),
        2,
        "the transaction sees its own staged write"
    );
    assert_eq!(
        derived::label_count(&*base, eng.tenant, "BOOK").unwrap(),
        1,
        "the base snapshot is unaffected"
    );
}

#[test]
fn retype_moves_label_scoped_derived_rows_and_keeps_rel_catalog_exact() {
    let eng = engine();
    let a = eng
        .create_record("A", None, vec![("n".into(), FieldChange::Set(json!(5)))])
        .unwrap();
    let b = eng.create_record("B", None, vec![]).unwrap();
    eng.create_relationship("KNOWS", a, b, vec![]).unwrap();
    eng.retype_record(a, "A", "C").unwrap();
    assert_oracle(&eng, "after retype");
    let view = eng.store.read_view().unwrap();
    let pid = pid_of(&eng, "n");
    assert_eq!(derived::label_count(&view, eng.tenant, "A").unwrap(), 0);
    assert_eq!(derived::label_count(&view, eng.tenant, "C").unwrap(), 1);
    assert_eq!(
        derived::prop_support_label(&view, eng.tenant, OwnerKind::Record, pid, "C").unwrap(),
        1
    );
    assert_eq!(
        derived::presence_owners(
            &view,
            eng.tenant,
            OwnerKind::Record,
            "A",
            pid,
            k::NULLNESS_VALUE
        )
        .unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(
        derived::presence_owners(
            &view,
            eng.tenant,
            OwnerKind::Record,
            "C",
            pid,
            k::NULLNESS_VALUE
        )
        .unwrap(),
        vec![a.ordinal]
    );
    // G1: live maintenance and a from-scratch derivation agree on the
    // relationship catalog after a relabel (the oracle above), and the edge
    // is counted exactly once.
    let total: u64 = derived::rel_catalog(&view, eng.tenant)
        .unwrap()
        .iter()
        .map(|(_, _, _, count)| count)
        .sum();
    assert_eq!(total, 1);
    assert_eq!(
        derived::degree(&view, eng.tenant, AdjDirection::Out, a.ordinal).unwrap(),
        1
    );
}

#[test]
fn presence_separates_values_from_explicit_nulls() {
    let eng = engine();
    let v = eng
        .create_record("P", None, vec![("age".into(), FieldChange::Set(json!(1)))])
        .unwrap();
    let n = eng
        .create_record("P", None, vec![("age".into(), FieldChange::ExplicitNull)])
        .unwrap();
    eng.create_record("P", None, vec![]).unwrap();
    let view = eng.store.read_view().unwrap();
    // The value lives on the typed definition and the explicit null on the
    // family's neutral Unresolved one; neither shows up under the other.
    let family = try_access::definition(&view, eng.tenant, pid_of(&eng, "age"))
        .unwrap()
        .unwrap()
        .family_id;
    let mut per_def = Vec::new();
    for def in try_access::definitions_of_family(&view, eng.tenant, family).unwrap() {

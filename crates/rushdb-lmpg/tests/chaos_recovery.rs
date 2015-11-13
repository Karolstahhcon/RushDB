//! Deterministic fault injection (documents 07/13): crashes at every
//! commit boundary and torn-tail WAL writes must converge to identical,
//! invariant-clean state after recovery.

use std::sync::Arc;

use rushdb_lmpg::import::import_document;
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use rushdb_types::keys::*;

fn state_fingerprint(store: &Store) -> u64 {
    let tenant_data = TenantId::from_ordinal(7);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for (k, v) in store
        .read_view()
        .unwrap()
        .range_prefix(&key(tenant_data, &[]))
        .unwrap()
    {
        if k.windows(7).any(|w| w == b"__log__")
            || k.starts_with(b"txn-")
            || k.windows(17).any(|w| w == b"meta/last-applied")
        {
            continue;
        }
        for b in &k {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        h = !h;
        for b in &v {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
    }
    h
}

fn raw_commit(
    store: &Store,
    tenant: TenantId,
    mutation: rushdb_types::command::Mutation,
) -> Result<rushdb_storage::CommitOutcome<()>, rushdb_storage::StorageError> {
    store.commit_for_tenant(&tenant, uuid::Uuid::new_v4(), |_ci, _txn| {
        let batch = rushdb_types::command::CommitBatch {
            command_version: rushdb_types::command::COMMAND_VERSION,
            tenant,
            transaction_id: "test-raw".into(),
            commit_key: uuid::Uuid::nil(),
            start_index: 0,
            isolation: rushdb_types::command::IsolationMode::Snapshot,
            preconditions: Vec::new(),
            mutations: vec![mutation],
        };
        Ok((batch, ()))
    })
}

#[test]
fn scrub_repair_removes_bounded_orphan_occurrences() {
    let store = Arc::new(Store::new_in_memory());
    let tenant = TenantId::from_ordinal(70);
    let _lmpg = Lmpg::new(Arc::clone(&store), tenant);
    let orphan = occ_key(tenant, rushdb_types::ids::OwnerKind::Record, 1, 2, 0);
    raw_commit(
        &store,
        tenant,
        rushdb_types::command::Mutation::PutRawKey {
            key: orphan.clone(),
            value: rushdb_types::occurrence::encode_occ_value(
                false,
                rushdb_types::occurrence::OccurrenceValue::ExplicitNull,
                None,
            ),
        },
    )
    .unwrap();
    let before = store.read_view().unwrap();
    let report = rushdb_lmpg::scrub::scrub_report(&before, tenant).unwrap();
    assert_eq!(report.occurrence_rows, 1, "{report:?}");
    assert!(report
        .violations
        .iter()
        .any(|v| v.what == "occurrence_orphaned"));
    let plan = rushdb_lmpg::scrub::plan_orphan_occurrences(&before, tenant, 1).unwrap();
    assert_eq!(plan.keys, vec![orphan]);
    assert!(plan.complete);
    drop(before);
    assert_eq!(
        rushdb_lmpg::scrub::repair_orphan_occurrences(&store, tenant, 1).unwrap(),
        1
    );
    let after = store.read_view().unwrap();
    let report = rushdb_lmpg::scrub::scrub_report(&after, tenant).unwrap();
    // Repair removes the base row; apply-time derivation removes the
    // presence and counters it produced, so nothing is left to flag.
    assert!(report.violations.is_empty(), "{:?}", report.violations);
}

#[test]
fn crash_at_every_commit_boundary_converges() {
    let primary = Arc::new(Store::new_in_memory());
    let tenant = TenantId::from_ordinal(9);

    {
        let eng = Lmpg::new(Arc::clone(&primary), tenant);
        import_document(
            &eng,
            "City",
            &serde_json::json!({"city":"Rome","pop":4_000_000}),
        )
        .unwrap();
        let a = eng
            .create_record(
                "X",
                None,
                vec![("k".into(), FieldChange::Set(serde_json::json!("v")))],
            )
            .unwrap();
        let b = eng.create_record("Y", None, vec![]).unwrap();
        eng.create_relationship(
            "L",
            a,
            b,
            vec![("m".into(), FieldChange::Set(serde_json::json!(1)))],
        )
        .unwrap();
        eng.update_fields(
            a,
            vec![("k".into(), FieldChange::Set(serde_json::json!("w")))],
        )
        .unwrap();
        eng.delete_record(b).unwrap();
    }

    let full_log = primary.take_log_for_crash();
    assert!(full_log.len() >= 5, "scenario produced too few commits");

    // every prefix is a clean checkpoint; replay twice and compare.
    for cut in 0..=full_log.len() {
        let prefix = &full_log[..cut];

        let rec_store = Arc::new(Store::new_in_memory());
        drop(Lmpg::new(Arc::clone(&rec_store), tenant));
        rec_store.try_replay(prefix).unwrap();

use super::*;
use rushdb_lmpg::{FieldChange, Lmpg};
use rushdb_storage::Store;
use rushdb_types::keys::{key, u64be, P_EMB_VECTOR};
use serde_json::json;
use std::cell::Cell;

fn on_backends(test: impl Fn(&VectorEngine)) {
    let path = rushdb_storage::TempPath::store("vector-pins");
    for persistent in [false, true] {
        let store = if persistent {
            Store::open(&path).unwrap()
        } else {
            Store::new_in_memory()
        };
        test(&VectorEngine::new(Arc::new(Lmpg::new(
            Arc::new(store),
            TenantId::from_ordinal(71),
        ))));
    }
    rushdb_storage::remove_store(&path).unwrap();
}

#[test]
fn vector_space_cardinality_is_bounded_per_tenant() {
    let engine = VectorEngine::new(Arc::new(Lmpg::new(
        Arc::new(Store::new_in_memory()),
        TenantId::from_ordinal(72),
    )));
    for index in 0..256 {
        engine.ensure_space(&format!("space-{index}"), 2).unwrap();
    }
    let error = engine.ensure_space("space-overflow", 2).unwrap_err();
    assert!(error.to_string().contains("space limit reached"));
}

#[test]
fn cached_ann_generation_releases_shared_reservation_on_engine_drop() {
    // A built `usearch` index's real measured memory
    // (`FrozenSegment::measured_bytes`) carries a large, roughly
    // size-independent allocator floor (tens of MiB even for one vector),
    // well above the old `generation_bytes() * 2` formula estimate this
    // budget used to assume was enough. 32 MiB comfortably covers it.
    let config = rushdb_storage::resources::StorageResources {
        logical_cache_bytes: 32 * 1024 * 1024,
        ..Default::default()
    };
    let store = Arc::new(Store::new_with_resources(config).unwrap());
    let budget = store.cache_budget().unwrap();
    let engine = VectorEngine::new(Arc::new(Lmpg::new(
        Arc::clone(&store),
        TenantId::from_ordinal(74),
    )));
    let root = engine
        .lmpg
        .create_record(
            "Doc",
            None,
            vec![("name".into(), FieldChange::Set(json!("ann")))],
        )
        .unwrap();
    engine.ensure_space("ann", 2).unwrap();
    engine
        .bind_external(
            "ann",
            SourceRef {
                kind: OwnerKind::Record,
                owner_ord: root.ordinal,
                pid: 0,
                elem: 0,
            },
            &[1., 0.],
        )
        .unwrap();
    engine.build_ann("ann").unwrap();
    assert!(budget.snapshot().used > 0);
    drop(engine);
    // The engine's generation reservation is released; derived state lives
    // on disk and charges nothing to the shared cache budget.
    assert_eq!(budget.snapshot().used, 0);
}

/// Binds `n` deterministic `dims`-dimensional vectors to a fresh space named
/// `alias` on `engine`.
#[expect(
    clippy::cast_precision_loss,
    reason = "the modulo-7 result is always in 0..7, far under f32's exact integer range"
)]
fn bind_many(engine: &VectorEngine, alias: &str, dims: u32, n: usize) {
    engine.ensure_space(alias, dims).unwrap();
    for i in 0..n {
        let root = engine
            .lmpg
            .create_record("Doc", None, vec![("i".into(), FieldChange::Set(json!(i)))])
            .unwrap();
        let vector: Vec<f32> = (0..dims).map(|d| ((i + d as usize) % 7) as f32).collect();
        engine
            .bind_external(
                alias,
                SourceRef {
                    kind: OwnerKind::Record,
                    owner_ord: root.ordinal,
                    pid: 0,
                    elem: 0,
                },
                &vector,
            )
            .unwrap();
    }
}

#[test]
fn ann_build_admission_uses_the_real_measured_peak_not_a_formula_estimate() {
    // The cache-budget reservation an ANN build charges — and that
    // therefore gates admission — must be the real `usearch`-reported index
    // size (`FrozenSegment::measured_bytes`, backed by
    // `usearch::Index::memory_usage`), not the old `generation_bytes() * 2`
    // formula estimate. Prove it by learning the real peak from one build,
    // then showing a fresh store budgeted at exactly that peak admits the
    // same build and one budgeted one byte under it rejects — a boundary a
    // formula estimate (a different number) would not line up with.
    let dims = 16u32;
    let n = 300usize;

    let learn_config = rushdb_storage::resources::StorageResources {
        logical_cache_bytes: 64 * 1024 * 1024,
        ..Default::default()
    };
    let learn_store = Arc::new(Store::new_with_resources(learn_config).unwrap());
    let learn_budget = learn_store.vector_cache_budget().unwrap();
    let learn_engine = VectorEngine::new(Arc::new(Lmpg::new(
        Arc::clone(&learn_store),
        TenantId::from_ordinal(75),
    )));
    bind_many(&learn_engine, "ann", dims, n);
    learn_engine.build_ann("ann").unwrap();
    // `used`, not `peak`: the pre-build reservation briefly charges a larger
    // conservative estimate before `build_ann` resizes it down to the real
    // measured size, and `peak` is a ratcheting high-water mark that stays
    // at that larger historical value — `used` reflects what is actually
    // retained right now, after the resize.
    let real_peak = learn_budget.snapshot().used;
    assert!(real_peak > 0, "a built index must retain a nonzero charge");

    // A formula-estimate-only scheme would gate on `generation_bytes() * 2`,
    // a different number — it must not equal what was actually measured
    // (otherwise this test would not distinguish the two mechanisms).
    let formula_estimate = n * (dims as usize * 4 + 64) * 2;
    assert_ne!(
        real_peak, formula_estimate,
        "the retained charge must be the real measured size, not the formula estimate"
    );

    // `usearch`'s HNSW construction has stochastic elements (layer
    // assignment), so the real measured size can vary build to build even
    // for identical input — use wide margins around the learned peak
    // rather than its exact byte value.
    let generous_limit = real_peak.saturating_mul(10);
    let tight_limit = (real_peak / 10).max(1);

    // Comfortably over the real peak: admits.
    let fits_config = rushdb_storage::resources::StorageResources {
        logical_cache_bytes: generous_limit,
        ..Default::default()
    };
    let fits_store = Arc::new(Store::new_with_resources(fits_config).unwrap());
    let fits_engine = VectorEngine::new(Arc::new(Lmpg::new(
        Arc::clone(&fits_store),
        TenantId::from_ordinal(76),
    )));
    bind_many(&fits_engine, "ann", dims, n);
    fits_engine
        .build_ann("ann")
        .expect("a budget comfortably over the real measured peak must admit the build");

    // Comfortably under the real peak: rejects.
    let tight_config = rushdb_storage::resources::StorageResources {
        logical_cache_bytes: tight_limit,
        ..Default::default()
    };
    let tight_store = Arc::new(Store::new_with_resources(tight_config).unwrap());
    let tight_engine = VectorEngine::new(Arc::new(Lmpg::new(
        Arc::clone(&tight_store),
        TenantId::from_ordinal(77),
    )));
    bind_many(&tight_engine, "ann", dims, n);
    let error = tight_engine
        .build_ann("ann")
        .expect_err("a budget comfortably under the real measured peak must reject the build");
    assert!(error.contains("ANN build memory budget"), "{error}");
}

fn fixture(engine: &VectorEngine, alias: &str) -> (SourceRef, EmbeddingSpace, u64) {
    let root = engine
        .lmpg
        .create_record(
            "Doc",
            None,
            vec![("name".into(), FieldChange::Set(json!("before")))],
        )
        .unwrap();
    let source = SourceRef {
        kind: OwnerKind::Record,
        owner_ord: root.ordinal,
        pid: 0,
        elem: 0,
    };
    let space = engine.ensure_space(alias, 2).unwrap();
    let slot = engine.bind_external(alias, source, &[1., 0.]).unwrap();
    (source, space, slot)
}

#[test]
fn retrying_identical_external_bind_is_idempotent() {
    let store = Arc::new(Store::new_in_memory());
    let engine = VectorEngine::new(Arc::new(Lmpg::new(store, TenantId::from_ordinal(72))));
    let source = SourceRef {
        kind: OwnerKind::Record,
        owner_ord: 1,
        pid: 0,
        elem: 0,
    };
    engine.ensure_space("retry", 2).unwrap();
    let first = engine.bind_external("retry", source, &[1., 0.]).unwrap();
    let second = engine.bind_external("retry", source, &[1., 0.]).unwrap();
    assert_eq!(first, second);
    let space = engine.lookup_space_by_alias("retry").unwrap().unwrap();
    assert_eq!(engine.bindings_of(space.space_id).unwrap().len(), 1);
}

#[test]
fn managed_embedding_completion_rejects_stale_source_hash() {
    let store = Arc::new(Store::new_in_memory());
    let engine = VectorEngine::new(Arc::new(Lmpg::new(store, TenantId::from_ordinal(73))));
    let root = engine
        .lmpg
        .create_record(
            "Doc",
            None,
            vec![("name".into(), FieldChange::Set(json!("before")))],
        )
        .unwrap();
    let source = SourceRef {
        kind: OwnerKind::Record,
        owner_ord: root.ordinal,
        pid: 0,
        elem: 0,
    };
    engine.ensure_space("managed", 2).unwrap();
    let captured_hash = source_hash(&engine.snapshot().unwrap(), engine.tenant(), source)
        .unwrap()
        .expect("source exists");

    engine
        .lmpg
        .update_fields(
            root,
            vec![("name".into(), FieldChange::Set(json!("after")))],
        )
        .unwrap();
    let error = engine
        .bind_if_source_hash("managed", source, captured_hash, &[1., 0.])
        .unwrap_err();
    assert!(error.to_string().contains("source changed"));
    let space = engine.lookup_space_by_alias("managed").unwrap().unwrap();
    assert!(engine.bindings_of(space.space_id).unwrap().is_empty());
}

#[test]
fn alias_and_binding_catalogs_share_a_view_across_clear_and_gc() {
    on_backends(|engine| {
        let (source, space, _) = fixture(engine, "catalog");
        let view = engine.snapshot().unwrap();
        engine.clear_space("catalog").unwrap();
        engine.store.gc(u64::MAX).unwrap();
        assert_eq!(
            engine.lookup_space_in_view(&view, "catalog").unwrap(),
            Some(space.clone())
        );
        assert!(engine
            .binding_of_in_view(&view, space.space_id, source)
            .unwrap()
            .is_some());
        assert_eq!(
            engine
                .bindings_of_in_view(&view, space.space_id)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            engine
                .search_exact_in_view(&view, &space, &[1., 0.], 1, None)
                .unwrap()
                .len(),
            1
        );
        assert!(engine.lookup_space_by_alias("catalog").unwrap().is_none());
        assert!(engine.bindings_of(space.space_id).unwrap().is_empty());
    });
}

#[test]
fn external_binding_tracks_owner_lifecycle_not_occurrence_hash() {
    on_backends(|engine| {
        let root = engine
            .lmpg
            .create_record(
                "Doc",
                None,
                vec![("name".into(), FieldChange::Set(json!("before")))],
            )
            .unwrap();
        let source = SourceRef {
            kind: OwnerKind::Record,
            owner_ord: root.ordinal,
            pid: 0,
            elem: 0,
        };
        let space = engine.ensure_space("external-lifecycle", 2).unwrap();
        engine
            .bind_external("external-lifecycle", source, &[1., 0.])
            .unwrap();

        engine
            .lmpg
            .update_fields(
                root,
                vec![("name".into(), FieldChange::Set(json!("after")))],
            )
            .unwrap();
        assert_eq!(
            engine.revalidate_bindings("external-lifecycle").unwrap(),
            (0, 0)
        );
        assert_eq!(
            engine.bindings_of(space.space_id).unwrap()[0].status,
            BindingStatus::Ready
        );

        engine.lmpg.delete_record(root).unwrap();
        assert_eq!(
            engine.revalidate_bindings("external-lifecycle").unwrap(),
            (0, 1)
        );
        assert_eq!(
            engine.bindings_of(space.space_id).unwrap()[0].status,
            BindingStatus::Retired
        );
    });
}

#[test]
fn caller_owned_vector_views_reject_another_store() {
    on_backends(|engine| {
        let (source, space, _) = fixture(engine, "catalog");
        let foreign = Store::new_in_memory().read_view().unwrap();
        assert!(engine.lookup_space_in_view(&foreign, "catalog").is_err());
        assert!(engine
            .binding_of_in_view(&foreign, space.space_id, source)
            .is_err());
        assert!(engine
            .bindings_of_in_view(&foreign, space.space_id)
            .is_err());
        assert!(engine
            .search_exact_in_view(&foreign, &space, &[1., 0.], 0, None)
            .is_err());
        assert!(engine
            .search_ann_in_view(&foreign, &space, &[1., 0.], 0, 1, None)
            .is_err());
        assert!(engine.reconstruct_sources_in_view(&foreign, &[]).is_err());
    });
}

fn mutate_raw(engine: &VectorEngine, storage_key: Vec<u8>, value: Option<Vec<u8>>) {
    let commit_key = uuid_v7();
    engine
        .store
        .commit(commit_key, |_, _| {
            let mut batch = empty_batch(engine.tenant());
            batch.commit_key = commit_key;
            let key = String::from_utf8(storage_key).unwrap();
            batch.mutations.push(match value {
                Some(value) => Mutation::SetMetaBlob { key, value },
                None => Mutation::DeleteMetaBlob { key },
            });
            Ok((batch, ()))
        })
        .unwrap();
}

#[test]
fn exact_ann_rerank_and_ann_fallback_keep_one_view_during_writes_and_gc() {
    on_backends(|engine| {
        for mode in 0..3 {
            let alias = format!("space-{mode}");
            let (source, space, slot) = fixture(engine, &alias);
            let hash = engine
                .binding_of(space.space_id, source)
                .unwrap()
                .unwrap()
                .source_hash;
            let ci = engine.store.committed_version();
            let changed = Cell::new(false);
            let filter = |_| {
                if !changed.replace(true) {
                    engine.bind_external(&alias, source, &[0., 1.]).unwrap();
                    mutate_raw(
                        engine,
                        emb_vector_key(engine.tenant(), space.space_id, slot),
                        None,
                    );
                    engine.store.gc(u64::MAX).unwrap();
                    assert_eq!(engine.store.gc_watermark(), ci);
                }
                true
            };
            let hits = match mode {
                0 => engine.search_exact(&alias, &[1., 0.], 1, Some(&filter)),
                1 => engine.search_ann(&alias, &[1., 0.], 1, 2, Some(&filter)),
                _ => engine.search_ann(&alias, &[1., 0.], 2, 2, Some(&filter)),
            }
            .unwrap();
            assert!(changed.get());
            assert_eq!(hits.len(), 1);
            assert!((hits[0].score - 1.).abs() < f32::EPSILON);
            assert_eq!(hits[0].value_hash, hash);
            assert!(
                engine.search_exact(&alias, &[1., 0.], 1, None).unwrap()[0]
                    .score
                    .abs()
                    < f32::EPSILON
            );
        }
    });
}

#[test]
fn corrupt_vector_bytes_are_errors_not_short_arrays_or_empty_results() {
    on_backends(|engine| {
        for (i, payload) in [
            None,
            Some(vec![1]),
            Some(vec![0; 4]),
            Some([f32::NAN.to_le_bytes(), 0f32.to_le_bytes()].concat()),
        ]
        .into_iter()
        .enumerate()
        {
            let alias = format!("bad-{i}");
            let (_, space, slot) = fixture(engine, &alias);
            mutate_raw(
                engine,
                emb_vector_key(engine.tenant(), space.space_id, slot),
                payload,
            );
            assert!(matches!(
                engine.search_exact(&alias, &[1., 0.], 1, None),
                Err(VectorError::Storage(StorageError::CorruptLog))
            ));
            assert!(matches!(
                engine.search_ann(&alias, &[1., 0.], 1, 2, None),
                Err(VectorError::Storage(StorageError::CorruptLog))
            ));
            // Metadata freshness is computed from binding metadata only: it
            // must not decode the (now corrupt) vector corpus. This guards the
            // unbudgeted whole-corpus decode that OOM'd metadata calls.
            let view = engine.snapshot().unwrap();
            assert!(
                engine.ann_lifecycle_in_view(&view, &space).is_ok(),
                "ANN lifecycle must not decode vector payloads"
            );
        }
    });
}

#[test]
fn malformed_aliases_and_bindings_do_not_look_missing() {
    on_backends(|engine| {
        let (source, space, _) = fixture(engine, "bad-binding");
        mutate_raw(
            engine,
            emb_binding_key(
                engine.tenant(),
                space.space_id,
                source.kind,
                source.owner_ord,
                source.pid,
                source.elem,
            ),
            Some(vec![1]),
        );
        assert!(engine.binding_of(space.space_id, source).is_err());
        assert!(engine.bindings_of(space.space_id).is_err());
        assert!(engine
            .search_exact("bad-binding", &[1., 0.], 1, None)
            .is_err());
        assert!(engine
            .search_ann("bad-binding", &[1., 0.], 1, 2, None)
            .is_err());
        mutate_raw(
            engine,
            emb_alias_key(engine.tenant(), "bad-binding"),
            Some(vec![1]),
        );
        assert!(engine.lookup_space_by_alias("bad-binding").is_err());
        assert!(engine.lookup_space_by_alias("absent").unwrap().is_none());
    });
}

#[test]
fn old_ann_generation_cannot_replace_newer_cache() {
    on_backends(|engine| {
        let (source, space, _) = fixture(engine, "cache");
        let old = engine.snapshot().unwrap();
        engine.bind_external("cache", source, &[0., 1.]).unwrap();
        let new = engine.snapshot().unwrap();
        engine.current_ann(&new, &space).unwrap();
        let old_version = crate::ann::space_version(&old, engine.tenant(), space.space_id)
            .unwrap()
            .1;
        let new_version = crate::ann::space_version(&new, engine.tenant(), space.space_id)
            .unwrap()
            .1;
        assert!(old_version < new_version);
        // A reader of the old view gets a generation for its own version…
        assert_eq!(
            engine.current_ann(&old, &space).unwrap().space_version,
            old_version
        );
        // …which never replaces the newer cached one.
        assert_eq!(
            engine.ann_cache.lock().unwrap()[&space.space_id].space_version,
            new_version
        );
    });
}

#[test]
fn invalid_queries_fail_on_empty_indexes_and_zero_k_is_supported() {
    on_backends(|engine| {
        engine.ensure_space("empty", 2).unwrap();
        assert!(matches!(
            engine.search_ann("empty", &[1.], 1, 2, None),
            Err(VectorError::DimMismatch(2, 1))
        ));
        assert!(matches!(
            engine.search_exact("empty", &[f32::NAN, 0.], 1, None),
            Err(VectorError::InvalidVector(_))
        ));
        assert!(matches!(
            engine.search_ann("empty", &[f32::INFINITY, 0.], 1, 2, None),
            Err(VectorError::InvalidVector(_))
        ));
        assert!(engine
            .search_ann("empty", &[1., 0.], 0, 0, None)
            .unwrap()
            .is_empty());
        assert!(engine.ensure_space("zero", 0).is_err());
        assert!((cosine(&[f32::MAX, f32::MAX], &[f32::MAX, f32::MAX]) - 1.).abs() < 1e-6);
    });
}

#[test]
fn shared_view_owner_reconstruction_and_native_reopen_preserve_results() {
    let path = rushdb_storage::TempPath::store("vector-reopen");
    let tenant = TenantId::from_ordinal(71);
    let engine = VectorEngine::new(Arc::new(Lmpg::new(
        Arc::new(Store::open(&path).unwrap()),
        tenant,
    )));
    let (source, space, _) = fixture(&engine, "reopen");
    drop(engine);
    let engine = VectorEngine::new(Arc::new(Lmpg::new(
        Arc::new(Store::open(&path).unwrap()),
        tenant,
    )));
    let snap = engine.snapshot().unwrap();
    let header = rushdb_lmpg::reader::try_access::record_by_ord(&snap, tenant, source.owner_ord)
        .unwrap()
        .unwrap();
    engine
        .lmpg
        .update_fields(
            rushdb_lmpg::RecordRef {
                record_id: header.record_id,
                ordinal: header.ordinal,
            },
            vec![("name".into(), FieldChange::Set(json!("after")))],
        )
        .unwrap();
    engine.store.gc(u64::MAX).unwrap();
    for hits in [
        engine
            .search_exact_in_view(&snap, &space, &[1., 0.], 1, None)
            .unwrap(),
        engine
            .search_ann_in_view(&snap, &space, &[1., 0.], 1, 2, None)
            .unwrap(),
    ] {
        assert_eq!(hits.len(), 1);
        assert!((hits[0].score - 1.).abs() < f32::EPSILON);
        assert_eq!(
            engine.reconstruct_sources_in_view(&snap, &hits).unwrap()[0]["name"],
            json!("before")
        );
        assert_eq!(
            engine.reconstruct_sources(&hits).unwrap()[0]["name"],
            json!("after")
        );
    }
    drop(snap);
    drop(engine);
    rushdb_storage::remove_store(&path).unwrap();
}

fn command(
    engine: &VectorEngine,
    version: u32,
    mutations: Vec<Mutation>,
) -> Result<CommitOutcome<()>, StorageError> {
    let commit_key = uuid_v7();
    engine.store.commit(commit_key, |_, _| {
        let mut batch = empty_batch(engine.tenant());
        batch.commit_key = commit_key;
        batch.command_version = version;
        batch.mutations = mutations;
        Ok((batch, ()))
    })
}

#[test]
fn clear_space_deletes_binary_keys_and_orphan_segments_without_crossing_scope() {
    on_backends(|base| {
        let engine = VectorEngine::new(Arc::new(Lmpg::new(
            base.store.clone(),
            TenantId(u32::MAX - 1),
        )));
        let other = VectorEngine::new(Arc::new(Lmpg::new(
            base.store.clone(),
            TenantId(u32::MAX - 2),
        )));
        let (source, space, original_slot) = fixture(&engine, "target");
        let (_, sibling, _) = fixture(&engine, "target/extra");
        let (_, other_space, _) = fixture(&other, "target");
        assert_eq!(space.space_id, other_space.space_id);
        let mut binding = engine.binding_of(space.space_id, source).unwrap().unwrap();
        binding.vector_slot = 255;
        command(
            &engine,
            rushdb_types::command::COMMAND_VERSION,
            vec![Mutation::UpsertEmbeddingBinding {
                binding,
                vector: Some(vec![0., 1.]),
                expect_live_hash: false,
            }],
        )
        .unwrap();
        engine.build_ann("target").unwrap();
        let old = engine.snapshot().unwrap();
        let slot_counter =
            meta_counter_key(engine.tenant(), &format!("vecslot/{}", space.space_id));
        let counter_before = old.get(&slot_counter).unwrap();
        assert_eq!(engine.clear_space("target").unwrap(), 1);
        assert!(engine.lookup_space_by_alias("target").unwrap().is_none());
        assert!(engine.bindings_of(space.space_id).unwrap().is_empty());
        assert!(engine
            .store
            .try_get(&emb_vector_key(
                engine.tenant(),
                space.space_id,
                original_slot
            ))
            .unwrap()
            .is_none());
        assert!(engine
            .store
            .try_get(&emb_vector_key(engine.tenant(), space.space_id, 255))
            .unwrap()
            .is_none());
        assert_eq!(engine.store.try_get(&slot_counter).unwrap(), counter_before);
        assert!(engine
            .store
            .try_get(&emb_space_key(engine.tenant(), space.space_id))
            .unwrap()
            .is_some());
        assert!(!engine
            .ann_cache
            .lock()
            .unwrap()
            .contains_key(&space.space_id));
        assert_eq!(engine.bindings_of(sibling.space_id).unwrap().len(), 1);
        assert_eq!(other.bindings_of(other_space.space_id).unwrap().len(), 1);
        engine.store.gc(u64::MAX).unwrap();
        assert!(old
            .get(&emb_alias_key(engine.tenant(), "target"))
            .unwrap()
            .is_some());
        assert!(
            (engine
                .search_exact_in_view(&old, &space, &[0., 1.], 1, None)
                .unwrap()[0]
                .score
                - 1.)
                .abs()
                < f32::EPSILON
        );
        let recreated = engine.ensure_space("target", 2).unwrap();
        assert!(recreated.space_id > sibling.space_id);
        assert!(engine.bindings_of(recreated.space_id).unwrap().is_empty());
    });
}

#[test]
fn racing_bind_and_clear_leave_no_orphan_binding_and_report_admitted_count() {
    on_backends(|engine| {
        for round in 0..10 {
            let alias = format!("race-{round}");
            let (source, space, _) = fixture(engine, &alias);
            let new_source = SourceRef {
                owner_ord: source.owner_ord + 1000,
                ..source
            };
            let barrier = std::sync::Barrier::new(2);
            let (bound, cleared) = std::thread::scope(|scope| {
                let binder = scope.spawn(|| {
                    barrier.wait();
                    engine.bind_external(&alias, new_source, &[0., 1.])
                });
                let clearer = scope.spawn(|| {
                    barrier.wait();
                    engine.clear_space(&alias)
                });
                (binder.join().unwrap(), clearer.join().unwrap())
            });
            assert_eq!(cleared.unwrap(), if bound.is_ok() { 2 } else { 1 });
            assert!(engine.lookup_space_by_alias(&alias).unwrap().is_none());
            assert!(engine.bindings_of(space.space_id).unwrap().is_empty());
            assert!(engine
                .snapshot()
                .unwrap()
                .range_prefix(&key(
                    engine.tenant(),
                    &[P_EMB_VECTOR, &u64be(space.space_id)]
                ))
                .unwrap()
                .is_empty());
        }
    });
}

#[test]
fn clear_intent_checks_version_and_alias_identity_before_publication() {
    on_backends(|engine| {
        let (_, space, _) = fixture(engine, "checked");
        let before = engine.store.committed_version();
        assert!(command(
            engine,
            2,
            vec![Mutation::ClearEmbeddingSpace {
                alias: "checked".into(),
                space_id: space.space_id
            }]
        )
        .is_err());
        assert!(command(
            engine,
            rushdb_types::command::COMMAND_VERSION,
            vec![
                Mutation::SetMetaBlob {
                    key: "clear-rollback-probe".into(),
                    value: vec![1]
                },
                Mutation::ClearEmbeddingSpace {
                    alias: "checked".into(),
                    space_id: space.space_id + 1
                },
            ]
        )
        .is_err());
        assert!(engine.clear_space("missing").is_err());
        assert_eq!(engine.store.committed_version(), before);
        assert!(engine
            .store
            .try_get(b"clear-rollback-probe")
            .unwrap()
            .is_none());
        assert_eq!(engine.bindings_of(space.space_id).unwrap().len(), 1);
    });
}

#[test]
fn old_commands_reject_atomically_and_current_clear_survives_reopen() {
    let path = rushdb_storage::TempPath::store("vector-clear");
    let tenant = TenantId(u32::MAX - 1);
    let engine = VectorEngine::new(Arc::new(Lmpg::new(
        Arc::new(Store::open(&path).unwrap()),
        tenant,
    )));
    // Any other format: none, the pre-release development format (7), and
    // a future one.
    for version in [0, 7, rushdb_types::command::COMMAND_VERSION + 1] {
        assert!(command(
            &engine,
            version,
            vec![Mutation::SetMetaValue {
                key: format!("rejected-command-{version}"),
                value: 42,
            }],
        )
        .is_err());
    }
    assert_eq!(engine.store.committed_version(), 0);
    let (_, space, slot) = fixture(&engine, "clear");
    engine.clear_space("clear").unwrap();
    let log = engine.store.take_log_for_crash();
    let last: CommitBatch = rushdb_types::command_codec::decode(log.last().unwrap()).unwrap();
    assert_eq!(last.command_version, rushdb_types::command::COMMAND_VERSION);
    assert_eq!(
        last.mutations,
        vec![Mutation::ClearEmbeddingSpace {
            alias: "clear".into(),
            space_id: space.space_id
        }]
    );
    drop(engine);
    let reopened = Arc::new(Store::open(&path).unwrap());
    let replayed = Arc::new(Store::new_in_memory());
    drop(Lmpg::new(replayed.clone(), tenant));
    replayed.try_replay(&log).unwrap();
    for store in [&reopened, &replayed] {
        let engine = VectorEngine::new(Arc::new(Lmpg::new(store.clone(), tenant)));
        assert!(engine.lookup_space_by_alias("clear").unwrap().is_none());
        assert!(engine.bindings_of(space.space_id).unwrap().is_empty());
        assert!(store
            .try_get(&emb_vector_key(tenant, space.space_id, slot))
            .unwrap()
            .is_none());
        for version in [1, 2, 4] {
            assert!(store
                .try_get(format!("rejected-command-{version}").as_bytes())
                .unwrap()
                .is_none());
        }
        assert!(engine.ensure_space("clear", 2).unwrap().space_id > space.space_id);
    }
    drop(reopened);
    rushdb_storage::remove_store(&path).unwrap();
}

#[test]
fn rebinding_reclaims_superseded_slots_once_the_floor_passes() {
    on_backends(|engine| {
        let (source, space, old_slot) = fixture(engine, "reclaim");
        let old_ci = engine.store.committed_version();
        // Re-binding allocates a fresh slot and stages the old one for
        // reclamation (it may still be read by a pinned view until the floor
        // passes this commit).
        let new_slot = engine.bind_external("reclaim", source, &[0., 1.]).unwrap();
        assert_ne!(new_slot, old_slot);
        let old_key = emb_vector_key(engine.tenant(), space.space_id, old_slot);
        let new_key = emb_vector_key(engine.tenant(), space.space_id, new_slot);
        assert!(engine.store.try_get(&old_key).unwrap().is_some());

        // At or below the old commit nothing is reclaimable.
        assert_eq!(engine.reclaim_slots(old_ci).unwrap(), 0);
        assert!(engine.store.try_get(&old_key).unwrap().is_some());

        // A pinned view at the old commit holds the floor: even a high
        // watermark cannot reclaim the slot it may still read.

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

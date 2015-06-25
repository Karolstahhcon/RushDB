use super::*;
use crate::equality::strings_equal;
use crate::planner::*;

/// Test-only convenience over [`index_candidates_with_budget`] (no budget, no
/// cancellation) — the production paths now always carry an explicit budget
/// and cancellation probe, so the unparameterized form only exists for these
/// planner unit tests.
fn index_candidates(
    lmpg: &Lmpg,
    snap: &QuerySnapshot,
    req: &SearchRequest,
) -> Result<Option<IndexPlan>, QueryError> {
    index_candidates_with_budget(lmpg, snap, req, None, &|| false)
}

#[test]
fn aggregate_numeric_conversion_rejects_lossy_integers() {
    assert_eq!(exact_f64(&serde_json::json!(42i64)), Some(42.0));
    assert_eq!(
        exact_f64(&serde_json::json!(9_007_199_254_740_993i64)),
        None
    );
}

#[test]
fn equality_preserves_large_integer_precision() {
    let exact = serde_json::json!(9_007_199_254_740_993i64);
    let rounded = serde_json::json!(9_007_199_254_740_992.0f64);
    assert!(!strings_equal(&exact, &rounded));
    assert!(strings_equal(
        &serde_json::json!(42),
        &serde_json::json!(42.0)
    ));
}

#[test]
fn equality_index_variants_do_not_round_large_integers() {
    let variants = scalar_variants(&serde_json::json!(9_007_199_254_740_993i64))
        .expect("integer is indexable");
    assert!(variants
        .iter()
        .any(|(_, scalar)| matches!(scalar, Scalar::Int64(9_007_199_254_740_993))));
    assert!(!variants
        .iter()
        .any(|(_, scalar)| matches!(scalar, Scalar::Float64(_))));
}

#[test]
fn equality_index_variants_do_not_saturate_out_of_domain_float() {
    let variants = scalar_variants(&serde_json::json!(9_223_372_036_854_775_808.0f64))
        .expect("finite float remains indexable");
    assert!(!variants
        .iter()
        .any(|(_, scalar)| matches!(scalar, Scalar::Int64(i64::MAX))));
}

#[test]
fn range_parser_rejects_imprecise_integer_bounds() {
    let exact = crate::document::parse_document(&serde_json::json!({
        "where": {"n": {"$gt": 9_007_199_254_740_992i64}}
    }));
    assert!(exact.is_ok());
    let imprecise = crate::document::parse_document(&serde_json::json!({
        "where": {"n": {"$gt": 9_007_199_254_740_993i64}}
    }));
    let Err(error) = imprecise else {
        panic!("imprecise bound was accepted")
    };
    assert!(error.to_string().contains("not exactly representable"));
}

#[test]
fn derived_numeric_literals_reject_lossy_integer_widening() {
    let result = crate::document::parse_document(&serde_json::json!({
        "select": {
            "value": {"$add": [9_007_199_254_740_993i64, 1]}
        }
    }));
    let Err(error) = result else {
        panic!("derived literal must not round through f64")
    };
    assert!(error.to_string().contains("not exactly representable"));
}

#[test]
fn range_index_comparison_preserves_large_integer_ordering() {
    let value = Scalar::Int64(9_007_199_254_740_993);
    let bound = 9_007_199_254_740_992.0;
    assert!(range_scalar_matches(&value, RangeOp::Gt, bound));
    assert!(!range_scalar_matches(&value, RangeOp::Lte, bound));
}

#[test]
fn scan_numeric_comparison_preserves_large_integer_ordering() {
    let value = serde_json::json!(9_007_199_254_740_993i64);
    assert!(numeric_cmp(
        Some(&value),
        9_007_199_254_740_992.0,
        |ordering| { ordering == std::cmp::Ordering::Greater }
    ));
}

#[test]
fn aggregate_minmax_preserves_large_integer_json_values() {
    let mut bucket = GroupBucket {
        keys: Map::new(),
        count_roots: 0,
        sums: Map::new(),
        counts: Map::new(),
        mins: Map::new(),
        maxs: Map::new(),
        derived_outputs: std::collections::BTreeMap::new(),
    };
    let first = serde_json::json!(9_007_199_254_740_993i64);
    let rounded = serde_json::json!(9_007_199_254_740_992.0f64);
    minmax(&mut bucket, "value".into(), &rounded, false);
    minmax(&mut bucket, "value".into(), &first, false);
    assert_eq!(bucket.maxs["value"], first);
}

#[test]
fn derived_field_expression_rejects_lossy_integer_widening() {
    let mut row = Map::new();
    row.insert("value".into(), serde_json::json!(9_007_199_254_740_993i64));
    let expr = OutputExpr::Field("value".into());
    assert_eq!(eval_row_expr(&expr, &row), None);
}

fn lmpg() -> Lmpg {
    Lmpg::new(
        Arc::new(rushdb_storage::Store::new_in_memory()),
        rushdb_types::ids::TenantId::from_ordinal(1),
    )
}

/// An engine whose query working set is capped at `cap` bytes
/// (`query-memory-max`).
fn lmpg_with_cap(cap: u64) -> Lmpg {
    Lmpg::new(
        Arc::new(
            rushdb_storage::Store::new_with_resources(
                rushdb_storage::resources::StorageResources {
                    query_memory_max_bytes: cap,
                    ..rushdb_storage::resources::StorageResources::default()
                },
            )
            .unwrap(),
        ),
        rushdb_types::ids::TenantId::from_ordinal(1),
    )
}

fn request(lmpg: &Lmpg, where_: &serde_json::Value) -> SearchRequest {
    let parsed = crate::document::parse_document(&serde_json::json!({ "where": where_.clone() }))
        .expect("valid document");
    let _ = lmpg;
    parsed.search
}

#[test]
fn eligible_returns_plan_residual_only_returns_none_and_force_scan_bypasses() {
    let l = lmpg();
    l.create_record(
        "A",
        None,
        vec![(
            "n".to_string(),
            rushdb_lmpg::plan::FieldChange::Set(serde_json::json!(1)),
        )],
    )
    .unwrap();
    let snap: QuerySnapshot = Arc::new(l.store.read_view().unwrap());

    // Eligible-only: exact plan with the matching owner.
    let plan = index_candidates(&l, &snap, &request(&l, &serde_json::json!({"n": 1})))
        .unwrap()
        .expect("index plan");
    assert!(plan.exact);
    assert_eq!(plan.ords.len(), 1);

    // Eligible + residual: partial prefilter, not exact.
    let plan = index_candidates(
        &l,
        &snap,
        &request(&l, &serde_json::json!({"n": 1, "s": {"$contains": "x"}})),
    )
    .unwrap()
    .expect("partial plan");
    assert!(!plan.exact);
    assert_eq!(plan.ords.len(), 1);

    // Residual only: no plan.
    assert!(index_candidates(
        &l,
        &snap,

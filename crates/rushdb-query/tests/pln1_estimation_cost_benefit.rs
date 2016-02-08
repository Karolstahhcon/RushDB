//! Value-distribution cost/benefit test suite: quantifies what the value-distribution
//! histogram's bounded, O(1)-maintained approximation actually buys at read
//! time, across realistic data shapes — the half of "is this worth it" that
//! neither of the two other value-distribution test files covers:
//!
//! - `rushdb-lmpg/tests/derived_state.rs`'s
//!   `derived_value_distribution_matches_a_base_scan_including_through_a_max_delete`
//!   proves the histogram's *exact* fields (min/max/distinct) are correct.
//!   It says nothing about how good the *approximate* bucket interpolation
//!   is for a real bound.
//! - `rushdb-lmpg/tests/pln1_ingest_overhead.rs` measures the *write-side*
//!   maintenance cost in isolation (ns/value) and asserts it is bounded.
//!   It says nothing about what that cost buys on the read side.
//!
//! This file measures the read-side payoff directly: estimation accuracy
//! against a ground-truth oracle, across distribution shapes and property
//! types, and the planner-visible consequence of that accuracy (does a
//! correct/incorrect estimate change whether a query takes the index path).
//!
//! # Headline finding (read before changing `VALUE_DISTRIBUTION_BUCKETS` or
//! the bucket-assignment formula)
//!
//! [`rushdb_lmpg::derived::Histogram`]'s 64 buckets are fixed over the
//! *full encoded-type domain* — deliberately never relative to the observed
//! `[min, max]`, so a bucket assignment survives deletes without rehashing
//! (see the design doc and `fixed_bucket_index`'s doc comment). The price
//! of that stability: for `Int64`/`DateTime` properties, whose
//! order-preserving encoding is a sign-flipped integer, the bucket is `(top
//! 6 bits of the 64-bit encoding)` — so *every* value in roughly `[-2^57,
//! 2^57)` (±144 quadrillion) falls in the *same single bucket*. Ages,
//! prices, scores, Unix-millisecond timestamps, row counts — virtually
//! every realistic integer property — lands entirely inside one of the 64
//! buckets. [`int64_distribution_shape_is_invisible_past_one_bucket`] below
//! demonstrates this concretely: three Int64 populations with deliberately
//! different shapes (uniform, skewed, clustered) produce statistically
//! indistinguishable histogram estimates, because the histogram cannot see
//! any structure finer than "everything is in bucket 32."
//! `Float64` fares much better
//! ([`float64_histogram_resolves_orders_of_magnitude_but_not_within_one`])
//! because IEEE-754's exponent-then-mantissa bit layout makes the top bits
//! of the encoding vary with *magnitude*, giving the same fixed-top-bits
//! trick a de facto logarithmic histogram — but it still collapses once
//! values are within the same order-of-magnitude band (demonstrated below).
//!
//! This is not a bug — `apply_transition`'s O(1)-per-value maintenance and
//! stable-under-delete bucket assignment are real, deliberate properties
//! the design doc traded resolution for — but it means the current
//! implementation's *actual* benefit for the single most common property
//! shape (bounded-range integers) is close to a 2-point (min/max) linear
//! estimate, not a "coarse distribution." See the module-end `Suggested
//! metrics` doc comment for how to track this going forward instead of a
//! single pass/fail accuracy bound.

use std::sync::Arc;
use std::time::Instant;

use rushdb_lmpg::derived;
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_query::document::parse_where;
use rushdb_storage::Store;
use rushdb_types::encoding::encode_order_preserving;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::model::{LogicalType, TypeState};
use rushdb_types::value::Scalar;
use serde_json::json;

fn engine(seed: u64) -> Lmpg {
    Lmpg::new(
        Arc::new(Store::new_in_memory()),
        TenantId::from_ordinal(5_000 + seed),
    )
}

/// Resolve the `pid` for a property typed `ty`, after at least one record
/// carrying it has been committed (mirrors `derived_state.rs`'s own
/// `pid_of_typed`, duplicated here since integration tests share no code).
fn pid_of_typed(eng: &Lmpg, name: &str, ty: LogicalType) -> u64 {
    use rushdb_lmpg::reader::keys_ext;
    use rushdb_types::value::normalize_name;
    let view = eng.store.read_view().unwrap();
    let sig = keys_ext::family_sig_key(
        eng.tenant,
        "default",
        OwnerKind::Record,
        &normalize_name(name),
    );
    let raw = view.get(&sig).unwrap().expect("family signature");
    let fid = u64::from_be_bytes(raw[..8].try_into().unwrap());
    let dkey = keys_ext::definition_key(eng.tenant, fid, TypeState::Known(ty));
    let def = view.get(&dkey).unwrap().expect("definition");
    u64::from_be_bytes(def[..8].try_into().unwrap())
}

fn ingest_int64(eng: &Lmpg, label: &str, name: &str, values: &[i64]) {
    for &v in values {
        eng.create_record(label, None, vec![(name.into(), FieldChange::Set(json!(v)))])
            .unwrap();
    }
}

fn ingest_float64(eng: &Lmpg, label: &str, name: &str, values: &[f64]) {
    for &v in values {
        eng.create_record(label, None, vec![(name.into(), FieldChange::Set(json!(v)))])
            .unwrap();
    }
}

/// Ground truth: exact count of `values` satisfying `>= bound` (`above`) or
/// `< bound` (`!above`) — a plain linear scan over the known fixture, which
/// is the oracle every histogram estimate below is checked against.
fn exact_count_i64(values: &[i64], bound: i64, above: bool) -> u64 {
    values
        .iter()
        .filter(|&&v| if above { v >= bound } else { v < bound })
        .count() as u64
}

fn exact_count_f64(values: &[f64], bound: f64, above: bool) -> u64 {
    values
        .iter()
        .filter(|&&v| if above { v >= bound } else { v < bound })
        .count() as u64
}

fn histogram_estimate_i64(eng: &Lmpg, pid: u64, bound: i64, above: bool) -> u64 {
    let view = eng.store.read_view().unwrap();
    let hist = derived::value_distribution(
        &view,
        eng.tenant,
        OwnerKind::Record,
        None,
        pid,
        derived::WalkBudget::default(),
    )
    .unwrap()
    .expect("histogram present");
    let enc = encode_order_preserving(LogicalType::Int64, &Scalar::Int64(bound)).unwrap();
    hist.estimate(&enc, above)
}

fn histogram_estimate_f64(eng: &Lmpg, pid: u64, bound: f64, above: bool) -> u64 {
    let view = eng.store.read_view().unwrap();
    let hist = derived::value_distribution(
        &view,
        eng.tenant,
        OwnerKind::Record,
        None,
        pid,
        derived::WalkBudget::default(),
    )
    .unwrap()
    .expect("histogram present");
    let enc = encode_order_preserving(LogicalType::Float64, &Scalar::Float64(bound)).unwrap();
    hist.estimate(&enc, above)
}

/// Widen a small, exact loop/count value into `f64` for a statistics
/// computation. Every caller in this file passes dataset sizes/counts in
/// the low thousands, far under `f64`'s 52-bit exact-integer range.
#[expect(
    clippy::cast_precision_loss,
    reason = "this file's counters are small test-fixture sizes, exact in f64 in practice"
)]
fn f64_from(value: u64) -> f64 {
    value as f64
}

/// Round `fraction * (len - 1)` to the nearest valid index into a slice of
/// length `len`. Used only for percentile lookups into a sorted fixture.
#[expect(
    clippy::cast_precision_loss,
    reason = "fixture lengths in this file are small test sizes, exact in f64 in practice"
)]
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "fraction is in [0.0, 1.0] and len > 0, so the rounded index is always in [0, len)"
)]
fn percentile_index(len: usize, fraction: f64) -> usize {
    (((len - 1) as f64) * fraction).round() as usize
}

/// Relative error of `estimate` against `exact`, as a fraction of the
/// dataset size `n` (not of `exact` itself, which would blow up to `inf`/
/// huge numbers whenever `exact` is near zero — a highly selective bound,
/// exactly the case a planner cares most about getting right). This is the
/// metric [`report`] and every accuracy assertion below uses.
fn relative_error(estimate: u64, exact: u64, n: u64) -> f64 {
    let diff = estimate.max(exact) - estimate.min(exact);
    f64_from(diff) / f64_from(n.max(1))
}

struct ErrorStats {
    mean: f64,
    median: f64,
    p95: f64,
    max: f64,
}

fn stats(mut errors: Vec<f64>) -> ErrorStats {
    errors.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = errors.len();
    let at = |p: f64| errors[percentile_index(n, p)];
    ErrorStats {
        mean: errors.iter().sum::<f64>() / f64_from(n as u64),
        median: at(0.5),
        p95: at(0.95),
        max: *errors.last().unwrap(),
    }
}

/// Deterministic LCG, matching the style already used by this crate's other
/// fixtures (`planner_differential.rs`) — no external RNG dependency.
fn lcg(seed: u64) -> impl FnMut() -> u64 {
    let mut state = seed;
    move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 33
    }
}

const N: usize = 4_000;

#[expect(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "span is always a small positive constant in this file, so the modulo result \
              fits i64 without wrapping"
)]
fn uniform_i64(seed: u64, span: i64) -> Vec<i64> {
    let mut next = lcg(seed);
    (0..N).map(|_| (next() % span as u64) as i64).collect()
}

/// Skewed/exponential-like: most mass packed near zero, a long thin tail —
/// the shape of e.g. request latencies or file sizes.
#[expect(
    clippy::cast_possible_truncation,

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

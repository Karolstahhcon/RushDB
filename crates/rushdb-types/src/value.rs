use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::cmp::Ordering;

use crate::model::LogicalType;

/// A single canonical scalar value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Scalar {
    /// Boolean value.
    Boolean(bool),
    /// Signed 64-bit integer.
    Int64(i64),
    /// IEEE-754 double-precision float.
    Float64(f64),
    /// UTF-8 string.
    Utf8(String),
    /// UTC epoch milliseconds.
    DateTime(i64),
}

/// Compare canonical scalars without widening integers through an imprecise
/// floating-point representation. Integer/float equality is accepted only
/// when the float is finite, integral, in range, and round-trips exactly.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::float_cmp,
    reason = "exact round-trip checks are the definition of canonical integer/float equality"
)]
pub fn scalar_equal(left: &Scalar, right: &Scalar) -> bool {
    match (left, right) {
        (Scalar::Boolean(a), Scalar::Boolean(b)) => a == b,
        (Scalar::Utf8(a), Scalar::Utf8(b)) => a == b,
        (Scalar::DateTime(a), Scalar::DateTime(b)) | (Scalar::Int64(a), Scalar::Int64(b)) => a == b,
        (Scalar::Float64(a), Scalar::Float64(b)) => a.is_finite() && b.is_finite() && a == b,
        (Scalar::Int64(integer), Scalar::Float64(float))
        | (Scalar::Float64(float), Scalar::Int64(integer)) => {
            if !float.is_finite() || float.fract() != 0.0 {
                return false;
            }
            // Rust saturates float-to-integer casts at the integer bounds.
            // Reject values outside the signed domain before casting so
            // `2^63` cannot alias `i64::MAX` (and likewise at the negative
            // boundary).
            if *float >= 9_223_372_036_854_775_808.0 || *float < -9_223_372_036_854_775_808.0 {
                return false;
            }
            let converted = *float as i64;
            converted == *integer && (converted as f64) == *float
        }
        _ => false,
    }
}

/// Compare canonical scalars without converting large integers to `f64`.
/// Mixed integer/float comparisons preserve the exact ordering represented by
/// the IEEE-754 value; non-numeric or mismatched domains return `None`.
#[must_use]
pub fn scalar_cmp(left: &Scalar, right: &Scalar) -> Option<Ordering> {
    match (left, right) {
        (Scalar::Boolean(a), Scalar::Boolean(b)) => Some(a.cmp(b)),
        (Scalar::Utf8(a), Scalar::Utf8(b)) => Some(a.cmp(b)),
        (Scalar::DateTime(a), Scalar::DateTime(b)) | (Scalar::Int64(a), Scalar::Int64(b)) => {
            Some(a.cmp(b))
        }
        (Scalar::Float64(a), Scalar::Float64(b)) => a.partial_cmp(b),
        (Scalar::Int64(integer), Scalar::Float64(float)) => cmp_int_float(*integer, *float),
        (Scalar::Float64(float), Scalar::Int64(integer)) => {
            cmp_int_float(*integer, *float).map(Ordering::reverse)
        }
        _ => None,
    }
}

/// Compare JSON scalar values using the canonical integer/float ordering.
/// Non-scalar values and mismatched domains return `None`.
#[must_use]
pub fn json_scalar_cmp(left: &Json, right: &Json) -> Option<Ordering> {
    let (_, left) = infer_scalar(left)?;
    let (_, right) = infer_scalar(right)?;
    scalar_cmp(&left, &right)
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "bounds and exact integer-part comparison are intentional"
)]
fn cmp_int_float(integer: i64, float: f64) -> Option<Ordering> {
    if !float.is_finite() {
        return float.partial_cmp(&0.0).map(Ordering::reverse);
    }
    let truncated = float.trunc();
    // Use the exact signed-domain boundary rather than `i64::MAX as f64`,
    // which rounds up to 2^63 and would make the saturating cast below alias
    // the largest signed integer.
    if truncated >= 9_223_372_036_854_775_808.0 {
        return Some(Ordering::Less);
    }
    if truncated < -9_223_372_036_854_775_808.0 {
        return Some(Ordering::Greater);
    }
    let integer_part = truncated as i64;
    match integer.cmp(&integer_part) {
        Ordering::Equal if float.fract() > 0.0 => Some(Ordering::Less),
        Ordering::Equal if float.fract() < 0.0 => Some(Ordering::Greater),
        ordering => Some(ordering),
    }
}

/// Normalize a property name for lookup: ASCII-uppercase folds to lowercase,
/// other alphanumerics are kept, and every other character becomes `_`.
///
/// # Examples
///
/// ```
/// use rushdb_types::value::normalize_name;
///
/// assert_eq!(normalize_name("FirstName"), "firstname");
/// assert_eq!(normalize_name("first-name"), "first_name");
/// ```
#[must_use]
pub fn normalize_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        if c.is_ascii_uppercase() {
            out.push(c.to_ascii_lowercase());
        } else if c.is_alphanumeric() {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    out
}

/// The record or relationship id in a reconstructed entity and in language
/// results (SQL, Cypher, GQL, Gremlin, Bolt).
pub const ID_FIELD: &str = "__id";
/// The record label in a reconstructed record.
pub const LABEL_FIELD: &str = "__label";
/// The relationship type in a reconstructed relationship.
pub const TYPE_FIELD: &str = "__type";

//! Equality between a reconstructed field value and a predicate literal, as
//! the evaluator applies it for `$eq`, `$ne`, `$in` and `$nin`.

use rushdb_types::value::{infer_scalar, scalar_equal};
use serde_json::Value as Json;

use crate::{DeclaredType, QueryError};

/// Apply a matcher to a scalar value or to every element of an array value
/// (`RushDB` array semantics: a condition matches when any element matches).
pub(crate) fn any_element(value: &Json, matcher: impl Fn(&Json) -> bool) -> bool {
    if let Json::Array(items) = value {
        items.iter().any(matcher)
    } else {
        matcher(value)
    }
}

pub(crate) fn is_datetime(

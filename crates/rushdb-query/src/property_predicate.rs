//! Predicates over one object's own properties, evaluated without the
//! graph around it (relationship properties): the full field-operator set
//! of the record predicate, but no traversal blocks.

use crate::document::{self, QueryDocumentError};
use crate::{eval_pred, Expr, Json, Map, QueryError};

/// Parse a property predicate that is evaluated on one object without the
/// graph around it (relationship properties): the full operator set, but
/// no traversal blocks.
///
/// # Errors
/// Returns [`QueryDocumentError::Parse`] for a malformed predicate or a
/// traversal block.
pub fn parse_property_predicate(where_: &Json) -> Result<Expr, QueryDocumentError> {
    fn traverses(expr: &Expr) -> bool {
        match expr {
            Expr::And(items) | Expr::Or(items) | Expr::Nor(items) => items.iter().any(traverses),
            Expr::Xor(a, b) => traverses(a) || traverses(b),
            Expr::Not(inner) => traverses(inner),
            Expr::Field(_) => false,
            Expr::Related(_) => true,

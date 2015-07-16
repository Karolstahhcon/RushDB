use serde::{Deserialize, Serialize};
use std::fmt;

/// The canonical scalar domain stored in postings and occurrences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogicalType {
    /// Boolean value.
    Boolean,
    /// Signed 64-bit integer.
    Int64,
    /// IEEE-754 double-precision float.
    Float64,

//! Checked integer `SUM`.
//!
//! `DataFusion`'s built-in `SUM(Int64)` accumulates with wrapping `i64`
//! addition, so an overflowing sum silently wraps to `i64::MIN` instead of
//! erroring. This module registers a `checked_sum` aggregate that returns an
//! execution error on overflow, and an optimizer rule that rewrites
//! `sum(<int64-expression>)` to it. Every other argument type keeps the
//! built-in `SUM`; `SUM(DISTINCT …)` is left to the built-in (the checked
//! accumulator does not deduplicate).

use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, Int64Type};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{
    udaf_default_display_name, udaf_default_schema_name, Accumulator, AggregateUDF,
    AggregateUDFImpl, ExprSchemable, Signature, Volatility,
};

/// `checked_sum(<int64>) -> <int64>`: exact, errors on overflow.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CheckedSumUdaf {
    signature: Signature,
}

impl CheckedSumUdaf {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CheckedSumUdaf {
    fn name(&self) -> &'static str {
        "checked_sum"
    }


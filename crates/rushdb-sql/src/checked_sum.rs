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

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CheckedSumAccumulator { sum: None }))
    }

    /// Present exactly as the built-in `SUM` so a rewritten plan's output
    /// column names are unchanged; downstream projections (and
    /// `optimize_projections`) still resolve `sum(<expr>)`.
    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(udaf_default_schema_name(self, params)?.replacen("checked_sum(", "sum(", 1))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(udaf_default_display_name(self, params)?.replacen("checked_sum(", "sum(", 1))
    }
}

#[derive(Debug)]
struct CheckedSumAccumulator {
    /// `None` until the first non-null input, so `SUM` of all-null stays null.
    sum: Option<i64>,
}

impl CheckedSumAccumulator {
    fn add(&mut self, value: i64) -> Result<()> {
        let current = self.sum.unwrap_or(0);
        self.sum =
            Some(current.checked_add(value).ok_or_else(|| {
                DataFusionError::Execution("integer SUM overflowed i64".to_string())
            })?);
        Ok(())
    }
}

impl Accumulator for CheckedSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Int64Type>();
        for index in 0..values.len() {
            if !values.is_null(index) {
                self.add(values.value(index))?;
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let values = states[0].as_primitive::<Int64Type>();
        for index in 0..values.len() {
            if !values.is_null(index) {
                self.add(values.value(index))?;
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(self.sum))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

/// Rewrite `sum(<int64-expression>)` to `checked_sum(<int64-expression>)`.
#[derive(Debug)]
pub(crate) struct CheckedSumRule {
    udaf: Arc<AggregateUDF>,
}

impl CheckedSumRule {
    pub(crate) fn new(udaf: Arc<AggregateUDF>) -> Self {
        Self { udaf }
    }
}

impl OptimizerRule for CheckedSumRule {
    fn name(&self) -> &'static str {
        "rushdb_checked_sum"
    }

    fn rewrite(
        &self,

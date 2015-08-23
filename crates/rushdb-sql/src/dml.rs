//! SQL-3: `INSERT`/`UPDATE`/`DELETE` on labels of any size.
//!
//! A DML statement is rewritten into the read query that finds the rows it
//! touches: `UPDATE` selects each matching row's `__id` with the old and new
//! value of every assigned column, `DELETE` selects the matching `__id`s, and
//! `INSERT` runs its `VALUES` or `SELECT` source. The read runs on the
//! streamed label tables of the read path, so its filter is planned on
//! the indexes like any `SELECT`, and only the touched rows are decoded. The
//! implied record writes are committed atomically at the snapshot version, as
//! before; their size (not the label's) is charged to the request budget.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, Expr, FromTable, Ident, ObjectName, SetExpr, Statement, TableFactor,
    TableObject, UpdateTableFromKind, Value,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use futures::StreamExt;
use rushdb_lmpg::reader::try_access as access;
use rushdb_lmpg::{BatchOp, BatchTx, FieldChange, Lmpg, RecordRef};
use rushdb_storage::ReadView;
use rushdb_textsearch::TextIndexes;
use rushdb_types::value::ID_FIELD;
use serde_json::Value as Json;

use crate::{
    append_batch_rows_with_budget, bind_parameters, canonical_uuid, context_in_view_with_budget,
    map_stream_error, memory_cap, planning_error, with_table_hint, Row, SqlError,
    SqlMutationResult,
};

/// Output column holding the touched row's `__id`.
const ROW_ID: &str = "__rushdb_id";
/// Output column prefixes for an `UPDATE` assignment's old and new value.
const OLD: &str = "__rushdb_old_";
const NEW: &str = "__rushdb_new_";
/// Fixed charge per planned write, beside its encoded values.
const WRITE_OVERHEAD: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DmlKind {
    Insert,
    Update,
    Delete,
}

/// A DML statement as the read that finds its rows.
#[derive(Debug)]
pub(crate) struct DmlPlan {
    pub(crate) kind: DmlKind,
    /// Target table, lowercased.
    pub(crate) target: String,
    /// The read query.
    pub(crate) query: String,
    /// `UPDATE`: the assigned properties, in assignment order. `INSERT`: the
    /// listed target columns (empty: the table's columns, in order).
    pub(crate) columns: Vec<String>,
    /// Bare placeholders bound directly to a target column, in placeholder
    /// order. `Describe` uses this to give the client the column's type when
    /// the rewritten read (a `VALUES` source or an assignment expression)
    /// cannot infer it on its own.
    pub(crate) parameter_columns: Vec<(u64, ParameterTarget)>,
}

/// Where a bare `$n` parameter's value lands in the target table.
#[derive(Debug, Clone)]
pub(crate) enum ParameterTarget {
    /// The column named by the statement's column list or assignment target.
    Column(String),
    /// The table's column at this position (an `INSERT` without a list).
    Position(usize),
}

fn unsupported(message: impl Into<String>) -> SqlError {
    SqlError::UnsupportedMutation(message.into())
}

/// The column an identifier names, as the planner resolves it: quoted
/// identifiers keep their case, unquoted ones are lowercased.
fn column_name(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn last_ident(name: &ObjectName) -> Result<&Ident, SqlError> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .ok_or_else(|| unsupported("computed column names are unsupported"))
}

/// How the select list refers to the target table: its alias, else its name.
fn qualifier(relation: &TableFactor) -> String {
    match relation {
        TableFactor::Table {
            alias: Some(alias), ..
        } => alias.name.to_string(),
        TableFactor::Table { name, .. } => name.to_string(),
        other => other.to_string(),
    }
}

/// Parse one DML statement and plan the read that finds its rows.
///
/// One match over the three statement kinds keeps each rewrite next to its
/// own validation.
///
/// # Errors
/// [`SqlError::UnsupportedMutation`] for anything but a single supported
/// `INSERT`, `UPDATE` or `DELETE` on one record table;
/// [`SqlError::InvalidIdentity`] for an `UPDATE` of `__id`.
#[expect(
    clippy::too_many_lines,
    reason = "one match arm per DML kind; each arm is its own rewrite"
)]
pub(crate) fn plan(sql: &str) -> Result<DmlPlan, SqlError> {
    let statements = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {})
        .map_err(|error| SqlError::Parse(error.to_string()))?;
    if statements.len() != 1 {
        return Err(unsupported(
            "exactly one INSERT, UPDATE, or DELETE statement is required",
        ));
    }
    let DFStatement::Statement(statement) = &statements[0] else {
        return Err(unsupported(statements[0].to_string()));
    };
    match statement.as_ref() {
        Statement::Insert(insert) => {
            let TableObject::TableName(name) = &insert.table else {
                return Err(unsupported("INSERT target must be a record table"));
            };
            let target = crate::unqualified_table_name(name)?;
            if insert.returning.is_some()
                || insert.on.is_some()
                || insert.or.is_some()
                || insert.ignore
                || insert.overwrite

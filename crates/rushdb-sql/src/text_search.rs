//! `text_search(index, query [, limit [, options]])`: the BM25 hits of a text
//! index as a table `(id TEXT, score DOUBLE)`, best first, searched at the
//! statement's pinned snapshot. Join it to a label table on `__id`:
//!
//! ```sql
//! SELECT d.title, t.score
//! FROM text_search('docs', 'quick fox', 20) AS t JOIN doc d ON d.__id = t.id
//! ORDER BY t.score DESC
//! ```
//!
//! `limit` defaults to 10; `options` is JSON text such as
//! `'{"allTerms": true}'` or `'{"phrase": true}'`. Arguments are literals
//! (the search runs while the statement is planned).

use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Expr;
use rushdb_lmpg::reader::try_access as access;
use rushdb_lmpg::Lmpg;
use rushdb_storage::ReadView;
use rushdb_textsearch::{LexicalOptions, TextIndexes, TextQuery};
use rushdb_types::ids::TenantId;

/// Hits returned when the call names no limit.
const DEFAULT_LIMIT: usize = 10;

/// Register `text_search` on `context`, searching `text` at `snapshot`.
/// Without text indexes (`None`) the function exists but every call fails,
/// so statements still plan the same way everywhere.
pub(crate) fn install(
    context: &SessionContext,
    lmpg: &Lmpg,
    snapshot: &Arc<ReadView>,
    text: Option<&Arc<TextIndexes>>,
) {
    context.register_udtf(
        "text_search",
        Arc::new(TextSearchFunction {
            text: text.cloned(),
            tenant: lmpg.tenant,
            snapshot: Arc::clone(snapshot),
        }),
    );
}

/// A `text_search` call that was invalid or whose search failed.
#[derive(Debug)]
pub(crate) struct TextSearchError(String);

impl std::fmt::Display for TextSearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TextSearchError {}

fn failed<T>(message: String) -> Result<T> {
    Err(DataFusionError::External(Box::new(TextSearchError(
        message,
    ))))
}

struct TextSearchFunction {
    text: Option<Arc<TextIndexes>>,

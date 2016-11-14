//! CSV ingest and export (`ingest.csv`, `query.exportCsv`).
//!
//! Decoding reproduces Papa Parse 5.4 followed by the platform's header
//! clean-up, which is what CSV imports meant before CSV moved into the
//! engine, so an existing import keeps its meaning: the same rows, property
//! names, typed values and errors. Export renders records the way Papa
//! Parse's `unparse` rendered the platform's export.
//!
//! Known differences: a quote or escape character must be one character
//! (Papa Parse accepts longer strings and then never recognises a quoted
//! field), and a delimiter containing a line break or the quote character is
//! rejected instead of being guessed.

use std::collections::{BTreeSet, HashMap, HashSet};

use rushdb_protocol::{CsvParseConfig, CsvSkipEmptyLines};
use serde_json::{Map, Value};

use super::{
    execution_failed, ErrorCode, ProtocolError, ProtocolExecutor, QueryDocument, Response,
};

/// Papa Parse's `dynamicTyping` turns a number into a JS number only while it
/// stays below 2^53 in magnitude.
const MAX_DYNAMIC_FLOAT: f64 = 9_007_199_254_740_992.0;
const BYTE_ORDER_MARK: char = '\u{feff}';

impl ProtocolExecutor<'_> {
    /// `query.exportCsv`: the records `query` selects, rendered by [`render`].
    pub(super) fn export_csv(
        &self,
        query: &QueryDocument,
        is_cancelled: &dyn Fn() -> bool,
        execution_context: &mut rushdb_engine::ExecutionContext,
    ) -> Result<Response, ProtocolError> {
        if query.select.is_some() || !query.group_by.is_empty() || query.cursor_ttl_ms.is_some() {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "exportCsv renders records: select, groupBy and cursorTtlMs are not accepted",
            ));
        }
        match self.query_records_with_context(query, is_cancelled, Some(execution_context))? {
            Response::Records { records, .. } => Ok(Response::Csv {
                count: records.len() as u64,
                content: render(&records),
            }),
            _ => Err(execution_failed(
                "a records query answered with another response",
            )),
        }
    }
}

/// CSV rows as documents for `ingest.csv`: decoded, with entry `i` of
/// `vectors` (when non-empty) as row `i`'s `$vectors`.
pub(super) fn csv_documents(
    data: &str,
    config: &CsvParseConfig,
    options: &Value,
    vectors: Vec<Value>,
) -> Result<Vec<Value>, ProtocolError> {
    let suggest_types = options
        .get("suggestTypes")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut rows = decode(data, config, suggest_types).map_err(|error| {
        ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("CSV parse error: {error}"),
        )
    })?;
    if vectors.len() > rows.len() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!(
                "vectors length ({}) exceeds the number of CSV data rows ({})",
                vectors.len(),
                rows.len()
            ),
        ));
    }
    for (row, entries) in rows.iter_mut().zip(vectors) {
        let has_entries = entries
            .as_array()
            .is_some_and(|entries| !entries.is_empty());
        if let (true, Some(row)) = (has_entries, row.as_object_mut()) {
            row.insert("$vectors".to_string(), entries);
        }
    }
    Ok(rows)
}

/// Decode CSV text into one JSON object per data row.
///
/// `suggest_types` is the `suggestTypes` import option, the default for
/// `dynamicTyping`.
///
/// # Errors
/// Returns the first decoding error, worded as Papa Parse words it (an
/// unterminated or malformed quoted field, or a row whose field count differs
/// from the header's), or an invalid configuration.
pub(super) fn decode(
    text: &str,
    config: &CsvParseConfig,
    suggest_types: bool,
) -> Result<Vec<Value>, String> {
    let quote = single_char(&config.quote_char, "quoteChar")?;
    let escape = match &config.escape_char {
        Some(escape) => single_char(escape, "escapeChar")?,
        None => quote,
    };
    let delimiter = config.delimiter.as_str();
    if delimiter.is_empty() || delimiter.contains(['\r', '\n', quote, BYTE_ORDER_MARK]) {
        return Err(
            "delimiter must be non-empty and contain no line break, quote character or byte order mark"
                .to_string(),
        );
    }
    let dynamic_typing = config.dynamic_typing.unwrap_or(suggest_types);

    let text = text.strip_prefix(BYTE_ORDER_MARK).unwrap_or(text);
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let newline = match config.newline.as_deref() {
        Some(newline @ ("\n" | "\r" | "\r\n")) => newline,
        _ => guess_line_endings(text, quote),
    };
    let renamed;
    let text = if config.header {
        renamed = rename_duplicate_headers(text, delimiter, newline);
        renamed.as_deref().unwrap_or(text)
    } else {
        text
    };

    let mut rows = Tokenizer {
        input: text,
        delimiter,
        newline,
        quote,
        escape,
    }
    .rows()?;
    match config.skip_empty_lines {
        CsvSkipEmptyLines::Keep => {}
        CsvSkipEmptyLines::Empty => rows.retain(|row| !(row.len() == 1 && row[0].is_empty())),
        CsvSkipEmptyLines::Greedy => rows.retain(|row| !js_trim(&row.concat()).is_empty()),
    }

    // `None` is Papa Parse's `null` (an empty cell under dynamic typing),
    // which the platform left out of a header row's object.
    let typed = |value: String| {
        if dynamic_typing {
            dynamic_value(value)
        } else {
            Some(Value::String(value))
        }
    };
    if !config.header {
        return Ok(rows
            .into_iter()
            .map(|row| {
                Value::Object(
                    row.into_iter()
                        .enumerate()
                        .map(|(index, value)| {
                            (
                                format!("column_{}", index + 1),
                                typed(value).unwrap_or(Value::Null),
                            )
                        })
                        .collect(),
                )
            })
            .collect());
    }

    let mut rows = rows.into_iter();
    let Some(fields) = rows.next() else {
        return Ok(Vec::new());
    };
    header_rows(&fields, rows.collect(), typed)
}

/// Data rows as objects keyed by the header's property names. A row whose
/// field count differs from the header's is an error.
fn header_rows(
    fields: &[String],
    rows: Vec<Vec<String>>,
    typed: impl Fn(String) -> Option<Value>,
) -> Result<Vec<Value>, String> {
    for row in &rows {

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
        if row.len() != fields.len() {
            let kind = if row.len() < fields.len() {
                "Too few"
            } else {
                "Too many"
            };
            return Err(format!(
                "{kind} fields: expected {} fields but parsed {}",
                fields.len(),
                row.len()
            ));
        }
    }
    let names = property_names(fields);
    Ok(rows
        .into_iter()
        .map(|row| {
            let mut object = Map::new();
            for ((field, name), value) in fields.iter().zip(&names).zip(row) {
                // A blank header cell names no property; an empty cell typed
                // to null is left out.
                if field.is_empty() {
                    continue;
                }
                if let Some(value) = typed(value) {
                    object.insert(name.clone(), value);
                }
            }
            Value::Object(object)
        })
        .collect())
}

fn single_char(value: &str, name: &str) -> Result<char, String> {
    let mut chars = value.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) if c != '\r' && c != '\n' => Ok(c),
        _ => Err(format!(
            "{name} must be a single character other than a line break"
        )),
    }
}

/// JavaScript's `String.prototype.trim`.
fn js_trim(value: &str) -> &str {
    value.trim_matches(|c: char| c.is_whitespace() || c == BYTE_ORDER_MARK)
}

/// Papa Parse's line-ending guess: `\n` unless `\r` comes first, then `\r\n`
/// when at least half of the `\r`-separated pieces start with `\n`. Quoted
/// text (from the first megabyte) is ignored.
fn guess_line_endings(text: &str, quote: char) -> &'static str {
    let mut end = text.len().min(1024 * 1024);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut unquoted = String::with_capacity(end);
    let mut rest = &text[..end];
    while let Some(open) = rest.find(quote) {
        let after = &rest[open + quote.len_utf8()..];
        match after.find(quote) {
            Some(close) => {
                unquoted.push_str(&rest[..open]);
                rest = &after[close + quote.len_utf8()..];
            }
            None => break,
        }
    }
    unquoted.push_str(rest);

    let r: Vec<&str> = unquoted.split('\r').collect();
    let n: Vec<&str> = unquoted.split('\n').collect();
    let n_appears_first = n.len() > 1 && n[0].len() < r[0].len();
    if r.len() == 1 || n_appears_first {
        return "\n";
    }
    let with_n = r.iter().filter(|piece| piece.starts_with('\n')).count();
    if with_n * 2 >= r.len() {
        "\r\n"
    } else {
        "\r"
    }
}

/// Papa Parse renames repeated header names on the raw first line (split on
/// the delimiter, quotes not considered): the second `a` becomes `a_1`, and a
/// name already taken gets `_<count>` appended until it is free.
fn rename_duplicate_headers(text: &str, delimiter: &str, newline: &str) -> Option<String> {
    let first_line = text.split(newline).next().unwrap_or("");
    let mut counts: HashMap<&str, usize> = HashMap::new();
    let mut names: Vec<String> = Vec::new();
    let mut duplicated = false;
    for header in first_line.split(delimiter) {
        let count = counts.get(header).copied().unwrap_or(0);
        let mut name = header.to_string();
        if count > 0 {
            duplicated = true;
            name = format!("{header}_{count}");
        }
        counts.insert(header, count + 1);
        while names.contains(&name) {
            name = format!("{name}_{count}");
        }
        names.push(name);
    }
    if !duplicated {
        return None;
    }
    let rest = &text[first_line.len()..];
    Some(format!("{}{rest}", names.join(delimiter)))
}

/// The platform's header clean-up: null bytes removed, trimmed, a blank name
/// becomes `column_<n>`, whitespace runs become `_`, a name not starting with
/// an ASCII letter or `_` gets a `_` prefix, and a repeated name gets `_2`,
/// `_3`, ...
fn property_names(fields: &[String]) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    fields
        .iter()
        .enumerate()
        .map(|(index, raw)| {
            let without_nul: String = raw.chars().filter(|c| *c != '\0').collect();
            let mut name = js_trim(&without_nul).to_string();
            if name.is_empty() {
                name = format!("column_{}", index + 1);
            }
            name = collapse_whitespace(&name);
            if !name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
                name = format!("_{name}");
            }
            if seen.contains(&name) {
                let base = name.clone();
                let mut counter = 2;
                while seen.contains(&format!("{base}_{counter}")) {
                    counter += 1;
                }
                name = format!("{base}_{counter}");
            }
            seen.insert(name.clone());
            name
        })
        .collect()
}

fn collapse_whitespace(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut in_space = false;
    for c in value.chars() {
        if c.is_whitespace() || c == BYTE_ORDER_MARK {
            if !in_space {
                out.push('_');
            }
            in_space = true;
        } else {
            out.push(c);
            in_space = false;
        }
    }
    out
}

/// Papa Parse's `dynamicTyping`: `true`/`TRUE`/`false`/`FALSE`, numbers below
/// 2^53 in magnitude, ISO datetimes with a time zone (normalised to UTC with
/// milliseconds; an invalid date is a JSON null that is kept), and the empty
/// string as `None` (Papa Parse's `null`). Everything else stays text.
fn dynamic_value(value: String) -> Option<Value> {
    match value.as_str() {
        "true" | "TRUE" => return Some(Value::Bool(true)),
        "false" | "FALSE" => return Some(Value::Bool(false)),
        "" => return None,
        _ => {}
    }
    if is_js_float(&value) {
        if let Ok(number) = js_trim(&value).parse::<f64>() {
            if number > -MAX_DYNAMIC_FLOAT && number < MAX_DYNAMIC_FLOAT {
                return Some(js_number_value(number));
            }
        }
    }
    match iso_datetime(&value) {
        IsoDatetime::Instant(instant) => return Some(Value::String(instant)),
        IsoDatetime::Invalid => return Some(Value::Null),
        IsoDatetime::NotIso => {}
    }
    Some(Value::String(value))
}

/// A JS number as JSON: integral values as integers (so `-0` is `0`).
fn js_number_value(number: f64) -> Value {
    if number.fract() == 0.0 && number.abs() < MAX_DYNAMIC_FLOAT {
        #[allow(clippy::cast_possible_truncation)]
        return Value::from(number as i64);
    }

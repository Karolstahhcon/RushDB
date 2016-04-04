//! MongoDB query language for rush-rushdb.
//!
//! Translates MongoDB-style documents onto the native query/mutate surface:
//! - `find` documents (filter → `where`, projection → `select`,
//!   sort → `orderBy`, limit/skip → pagination) — the native `where` grammar is
//!   already MongoDB-shaped.
//! - aggregate pipelines (subset: `$match/$group/$sort/$limit/$skip/$count`),
//!   folded into one native query: every `$match` must precede `$group`,
//!   `$limit` and `$skip`, and several are combined with `$and`.
//! - write commands (`insertOne/insertMany/updateOne/updateMany/
//!   deleteOne/deleteMany`, `upsert: true`).
//!
//! Index commands (`createIndex`/`dropIndex`/`reIndex`) are handled by the
//! server executor via the native index operations.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{BatchOp, BatchTx, Lmpg};
use rushdb_storage::ReadView;
use serde_json::{json, Map, Value};

mod text;

/// Execute a MongoDB read statement (find or aggregate) and return
/// `(total, rows)` where rows are objects, matching the statement-language
/// response shape.
///
/// # Errors
///
/// Returns an error string if the statement is not valid JSON, is neither an
/// object nor a pipeline array, uses an unsupported aggregate stage, or query
/// execution fails.
pub fn execute_query(
    lmpg: &Lmpg,
    statement: &str,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<(u64, Vec<Value>), String> {
    let snap = Arc::new(lmpg.store.read_view().map_err(|e| e.to_string())?);
    execute_query_in_view_with_budget(lmpg, snap, statement, is_cancelled, None, None)
}

/// Execute a MongoDB read over a caller-owned pinned view and optional working
/// byte budget. This is the transport path used by the Rust protocol boundary.
/// `text` answers `$text` filters.
///
/// # Errors
/// Returns a parse, storage, query, or resource-budget error.
pub fn execute_query_in_view_with_budget(
    lmpg: &Lmpg,
    snap: Arc<ReadView>,
    statement: &str,
    is_cancelled: &dyn Fn() -> bool,
    budget: Option<usize>,
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<(u64, Vec<Value>), String> {
    let doc: Value =
        serde_json::from_str(statement).map_err(|e| format!("mongo: invalid JSON: {e}"))?;
    if doc.is_array() {
        return execute_aggregate(lmpg, snap, &doc, is_cancelled, budget, text);
    }
    let obj = doc
        .as_object()
        .ok_or_else(|| "mongo: statement must be an object or pipeline array".to_string())?;
    if obj.contains_key("aggregate") {
        let pipeline = obj
            .get("aggregate")
            .and_then(Value::as_array)
            .ok_or_else(|| "mongo: `aggregate` must be an array".to_string())?;
        let collection = obj
            .get("collection")
            .and_then(Value::as_str)
            .unwrap_or_default();
        return execute_pipeline(lmpg, snap, collection, pipeline, is_cancelled, budget, text);
    }
    execute_find(lmpg, snap, obj, is_cancelled, budget, text)
}

/// Execute a MongoDB find document.
fn execute_find(
    lmpg: &Lmpg,
    snap: Arc<ReadView>,
    doc: &Map<String, Value>,
    is_cancelled: &dyn Fn() -> bool,
    budget: Option<usize>,
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<(u64, Vec<Value>), String> {
    let mut doc = doc.clone();
    let collection = doc
        .get("collection")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let mut scores = None;
    if let Some(Value::Object(filter)) = doc.get_mut("filter") {
        scores = text::take_text(lmpg, &snap, &collection, filter, text)?;
    }
    let score_fields = take_meta(&mut doc, "projection");
    let by_score = !take_meta(&mut doc, "sort").is_empty();
    let scores = match scores {
        Some(scores) => scores,
        None if score_fields.is_empty() && !by_score => {
            let query = translate_find(&doc)?;
            let page = find(lmpg, snap, &query, is_cancelled, budget)?;
            return Ok((page.total_matched, public_ids(page.records)));
        }
        None => return Err("mongo: `$meta: \"textScore\"` requires a `$text` filter".into()),
    };
    // Sorting by score pages after the sort: fetch every match.
    let page_bounds = by_score.then(|| (doc.remove("skip"), doc.remove("limit")));
    if by_score {
        doc.insert("limit".into(), json!(scores.len().max(1)));
    }
    let query = translate_find(&doc)?;
    let page = find(lmpg, snap, &query, is_cancelled, budget)?;
    let mut records = public_ids(page.records);
    let score_of = |record: &Value| {
        record
            .get("_id")
            .and_then(Value::as_str)
            .and_then(|id| scores.get(id))
            .copied()
            .unwrap_or(0.0)
    };
    if let Some((skip, limit)) = page_bounds {
        // Stable: equal scores keep the order of the remaining sort keys.
        records.sort_by(|a, b| score_of(b).total_cmp(&score_of(a)));
        let skip = skip.and_then(|v| v.as_u64()).unwrap_or(0);
        let skip = usize::try_from(skip).unwrap_or(usize::MAX);
        let limit = limit
            .and_then(|v| v.as_u64())
            .map_or(usize::MAX, |n| usize::try_from(n).unwrap_or(usize::MAX));
        records = records.into_iter().skip(skip).take(limit).collect();
    }
    for record in &mut records {
        let score = score_of(record);
        if let Some(fields) = record.as_object_mut() {
            for name in &score_fields {
                fields.insert(name.clone(), json!(score));
            }
        }
    }
    Ok((page.total_matched, records))
}

/// The native query `query`, run at `snap`.
fn find(
    lmpg: &Lmpg,
    snap: Arc<ReadView>,
    query: &Value,
    is_cancelled: &dyn Fn() -> bool,
    budget: Option<usize>,
) -> Result<rushdb_query::Page, String> {
    let parsed = rushdb_query::document::parse_document(query).map_err(|e| e.to_string())?;
    rushdb_query::find_records_in_view_with_budget(lmpg, &parsed.search, is_cancelled, snap, budget)
        .map_err(|e| e.to_string())
}

/// Take the `{"$meta": "textScore"}` entries out of `doc[key]` (dropping a
/// key left empty, which then means "every field" or "no sort"); their names.
fn take_meta(doc: &mut Map<String, Value>, key: &str) -> Vec<String> {
    let Some(Value::Object(fields)) = doc.get_mut(key) else {
        return Vec::new();
    };
    let names = text::take_text_score(fields);
    if !names.is_empty() && fields.is_empty() {
        doc.remove(key);
    }
    names
}

/// In MongoDB `_id` is the document's id: a top-level `_id` condition
/// matches the record identity (`$id`), not a property named `_id`.
fn id_filter(filter: &Value) -> Value {
    let Some(object) = filter.as_object() else {
        return filter.clone();
    };
    Value::Object(
        object
            .iter()
            .map(|(key, value)| {
                let key = if key == "_id" { "$id" } else { key.as_str() };
                (key.to_owned(), value.clone())
            })
            .collect(),
    )
}

/// Records carry the engine's `__id` as MongoDB's `_id`, in the wire form
/// every protocol surface uses: a dashed UUID.
fn public_ids(mut records: Vec<Value>) -> Vec<Value> {
    for record in &mut records {
        let Some(object) = record.as_object_mut() else {
            continue;
        };
        let id = object
            .remove(rushdb_types::value::ID_FIELD)
            .and_then(|raw| raw.as_str().and_then(|raw| uuid::Uuid::parse_str(raw).ok()));
        if let Some(id) = id {
            object.insert("_id".into(), Value::String(id.hyphenated().to_string()));
        }
    }
    records
}

/// Translate a Mongo find document to a native `QueryDocument`.
///
/// # Errors
///
/// Returns an error string if the document supplies neither `collection` nor
/// `labels`.
pub fn translate_find(doc: &Map<String, Value>) -> Result<Value, String> {
    let mut q = Map::new();
    let labels: Vec<String> = doc
        .get("collection")
        .and_then(Value::as_str)
        .map(|s| vec![s.to_string()])
        .or_else(|| {
            doc.get("labels").and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
        })
        .ok_or_else(|| "mongo: find requires `collection` or `labels`".to_string())?;
    q.insert(
        "labels".into(),
        Value::Array(labels.into_iter().map(Value::String).collect()),
    );
    if let Some(filter) = doc.get("filter") {
        q.insert("where".into(), id_filter(filter));
    }
    if let Some(proj) = doc.get("projection").and_then(Value::as_object) {
        let mut select = Map::new();
        for (k, v) in proj {
            let numeric = v.as_i64();
            if !matches!(numeric, Some(0 | 1)) && !v.is_boolean() {
                return Err("mongo: projection values must be true, false, 0, or 1".into());
            }
            if v == &Value::Bool(false) {
                return Err("mongo: exclusion projections are unsupported".into());
            }
            let include = v.as_i64() == Some(1)
                || v.as_i64() == Some(0) && k.ends_with("score") // keep score probes
                || v == &Value::Bool(true);
            if v.as_i64() == Some(0) && !k.ends_with("score") {
                return Err("mongo: exclusion projections are unsupported".into());
            }
            if include && v != &Value::Bool(true) {
                select.insert(k.clone(), Value::String(format!("$record.{k}")));
            }
        }
        q.insert("select".into(), Value::Object(select));
    }
    if let Some(sort) = doc.get("sort").and_then(Value::as_object) {
        let mut order = Map::new();
        for (k, v) in sort {
            let dir = mongo_sort_direction(v)?;
            order.insert(k.clone(), Value::String(dir.into()));
        }
        q.insert("orderBy".into(), Value::Object(order));
    }
    for key in ["limit", "skip"] {
        if let Some(v) = doc.get(key) {
            q.insert(key.into(), v.clone());
        }
    }
    Ok(Value::Object(q))
}

fn mongo_sort_direction(value: &Value) -> Result<&'static str, String> {
    match value.as_i64() {
        Some(-1) => Ok("desc"),
        Some(1) => Ok("asc"),
        _ => Err("mongo: sort direction must be 1 or -1".into()),
    }
}

/// Execute a Mongo aggregate pipeline (subset) via the native aggregate.
fn execute_aggregate(
    lmpg: &Lmpg,
    snap: Arc<ReadView>,
    pipeline: &Value,
    is_cancelled: &dyn Fn() -> bool,
    budget: Option<usize>,
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<(u64, Vec<Value>), String> {
    let arr = pipeline
        .as_array()
        .ok_or_else(|| "mongo: aggregate must be an array".to_string())?;
    // The first stage is typically $match; the collection is carried in the
    // statement object (handled by execute_pipeline). Here require a wrapper.
    execute_pipeline(lmpg, snap, "", arr, is_cancelled, budget, text)
}

/// Translate an aggregate pipeline to a native aggregate query document and
/// execute it.
fn execute_pipeline(
    lmpg: &Lmpg,
    snap: Arc<ReadView>,
    collection: &str,
    pipeline: &[Value],
    is_cancelled: &dyn Fn() -> bool,
    budget: Option<usize>,
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<(u64, Vec<Value>), String> {
    let mut where_clause = Map::new();
    let mut group_by: Vec<String> = Vec::new();
    let mut select = Map::new();
    let mut order = Map::new();
    let mut limit: Option<i64> = None;
    let mut skip: Option<i64> = None;
    let mut has_group = false;

    for (position, stage) in pipeline.iter().enumerate() {
        let obj = stage
            .as_object()
            .ok_or_else(|| "mongo: aggregate stage must be an object".to_string())?;
        if let Some(m) = obj.get("$match") {
            let m = &match_filter(lmpg, &snap, collection, position, m, text)?;
            // The pipeline runs as one query, whose filter applies before
            // grouping and paging; a later `$match` would mean something else.
            if has_group || limit.is_some() || skip.is_some() {
                return Err(
                    "mongo: `$match` after `$group`, `$limit` or `$skip` is unsupported".into(),
                );
            }
            and_match(&mut where_clause, m)?;
        } else if let Some(g) = obj.get("$group") {
            has_group = true;
            apply_group_stage(g, &mut group_by, &mut select)?;
        } else if let Some(s) = obj.get("$sort").and_then(Value::as_object) {
            if s.values().any(text::is_text_score) {
                return Err("mongo: sorting a pipeline by `textScore` is unsupported".into());
            }
            for (k, v) in s {
                let dir = mongo_sort_direction(v)?;
                order.insert(k.clone(), Value::String(dir.into()));
            }
        } else if let Some(l) = obj.get("$limit") {
            limit = l.as_i64();
        } else if let Some(s) = obj.get("$skip") {
            skip = s.as_i64();
        } else if let Some(c) = obj.get("$count") {
            if let Some(name) = c.as_str() {
                select.insert(name.to_string(), json!({ "$count": "*" }));
            }
        } else {
            return Err(format!(
                "mongo: unsupported aggregate stage: {}",
                obj.keys().next().unwrap_or(&String::new())
            ));
        }
    }

    let mut q = Map::new();
    let labels = if collection.is_empty() {
        return Err("mongo: aggregate requires a collection".into());
    } else {
        vec![collection.to_string()]
    };
    q.insert(
        "labels".into(),
        Value::Array(labels.into_iter().map(Value::String).collect()),
    );
    if !where_clause.is_empty() {
        q.insert("where".into(), Value::Object(where_clause));
    }
    if has_group {
        q.insert(
            "groupBy".into(),
            Value::Array(group_by.into_iter().map(Value::String).collect()),
        );
    }
    q.insert("select".into(), Value::Object(select));
    if !order.is_empty() {
        q.insert("orderBy".into(), Value::Object(order));
    }
    if let Some(skip) = skip {
        q.insert("skip".into(), Value::Number(skip.into()));
    }
    if let Some(limit) = limit {
        q.insert("limit".into(), Value::Number(limit.into()));
    }

    let parsed =
        rushdb_query::document::parse_document(&Value::Object(q)).map_err(|e| e.to_string())?;
    if let Some(aggregate) = parsed.aggregate {
        let (buckets, _matched) = rushdb_query::aggregate_with_cancel_in_view(
            lmpg,
            &aggregate,
            is_cancelled,
            Arc::clone(&snap),
        )
        .map_err(|e| e.to_string())?;
        let rows = rushdb_query::aggregate_to_rows(lmpg, &buckets, &aggregate)
            .map_err(|e| e.to_string())?;
        // Mongo aggregate `total` is the post-limit output row count.
        enforce_budget(&rows, budget)?;
        return Ok((rows.len() as u64, rows));
    }
    let page = rushdb_query::find_records_in_view_with_budget(
        lmpg,
        &parsed.search,
        is_cancelled,
        snap,
        budget,
    )
    .map_err(|e| e.to_string())?;
    enforce_budget(&page.records, budget)?;
    Ok((page.total_matched, public_ids(page.records)))
}

/// The filter of the `$match` at `position`, its `$text` (first stage only,
/// as in MongoDB) resolved to the matching records.
fn match_filter(
    lmpg: &Lmpg,
    snap: &ReadView,
    collection: &str,
    position: usize,
    stage: &Value,
    text: Option<&Arc<rushdb_textsearch::TextIndexes>>,
) -> Result<Value, String> {
    let mut stage = stage.clone();
    if let Some(filter) = stage.as_object_mut() {
        if position == 0 {
            text::take_text(lmpg, snap, collection, filter, text)?;
        } else if filter.contains_key("$text") {
            return Err("mongo: `$text` must be in the first `$match`".into());
        }
    }
    Ok(stage)
}

/// Combine a `$match` stage's filter with the filters of earlier stages.
fn and_match(where_clause: &mut Map<String, Value>, stage: &Value) -> Result<(), String> {
    let filter = stage
        .as_object()
        .cloned()
        .ok_or_else(|| "mongo: `$match` must be an object".to_string())?;
    if filter.is_empty() {

//! MongoDB `$text`: `{"$text": {"$search": "…"}}` at the top of a `find`
//! filter or of a pipeline's first `$match`, searched in the collection's
//! one text index, plus `{"$meta": "textScore"}` in a find's projection and
//! sort.
//!
//! The search runs once, at the statement's snapshot. Its matches restrict
//! the rest of the query (`$id $in`), so every other filter, sort, page and
//! group applies to them unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use rushdb_lmpg::reader::try_access as access;
use rushdb_lmpg::Lmpg;
use rushdb_storage::ReadView;
use rushdb_textsearch::{LexicalOptions, TextIndexes, TextQuery, ALL_HITS};
use rushdb_types::index::{IndexKind, IndexState};
use serde_json::{json, Map, Value};

/// The matches of a `$text` filter: record id → BM25 score.
pub(crate) type Scores = HashMap<String, f64>;

/// Take `$text` out of `filter`, search it and restrict `filter` to its
/// matches. `None` when the filter has no `$text`.
pub(crate) fn take_text(
    lmpg: &Lmpg,
    snap: &ReadView,
    collection: &str,
    filter: &mut Map<String, Value>,
    text: Option<&Arc<TextIndexes>>,
) -> Result<Option<Scores>, String> {
    let Some(spec) = filter.remove("$text") else {
        return Ok(None);
    };
    let search = parse_spec(&spec)?;
    let text = text.ok_or("mongo: `$text` is unavailable here")?;
    let index = collection_index(snap, lmpg, collection)?;
    let found = text.search(
        lmpg.tenant,
        snap,
        TextQuery {
            index: &index,
            query: &search,
            opts: LexicalOptions::default(),
            top_k: ALL_HITS,
        },
        None,
    )?;
    let mut scores = Scores::with_capacity(found.hits.len());
    for hit in found.hits {
        if let Some(header) =
            access::record_by_ord(snap, lmpg.tenant, hit.owner_ord).map_err(|e| e.to_string())?
        {
            scores.insert(
                uuid::Uuid::from_bytes(header.record_id)
                    .hyphenated()
                    .to_string(),
                hit.score,
            );
        }
    }
    let mut ids: Vec<&String> = scores.keys().collect();
    ids.sort();
    let matches = json!({ "$id": { "$in": ids } });
    if filter.is_empty() {
        *filter = matches.as_object().cloned().unwrap_or_default();
    } else {
        let rest = std::mem::take(filter);
        filter.insert("$and".into(), json!([Value::Object(rest), matches]));
    }
    Ok(Some(scores))
}

/// The `$search` string of a `$text` operator.

use std::collections::{BTreeSet, HashMap};

use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::model::{LogicalType, TypeState};
use rushdb_types::value::{infer_scalar, normalize_name, scalar_equal, Scalar};
use serde_json::{json, Value as Json};

use crate::plan::FieldChange;
use crate::reader::keys_ext as kx;
use crate::reader::TryRead;
use crate::{Lmpg, LmpgError, RecordRef};

/// Bounded Rust ingestion frontend (document 21 slice): decodes one JSON
/// document into format-neutral drafts and lowers them through the same
/// mutation path as every other writer. Nested objects and arrays of
/// objects promote to child records joined by relationships; scalar arrays
/// decompose into ordered element occurrences; `null` becomes a live
/// explicit-Null occurrence under the `NativeV1` profile.
///
/// # Errors
/// Returns [`LmpgError::Conflict`] if `doc` is not a JSON object or any
/// underlying record/relationship write fails.
pub fn import_document(lmpg: &Lmpg, label: &str, doc: &Json) -> Result<RecordRef, LmpgError> {
    import_document_with_cancel(lmpg, label, doc, &|| false)
}

/// Imports one document while polling a caller-owned cancellation probe before
/// each recursive record and relationship mutation.
///
/// # Errors
/// Returns [`LmpgError::Conflict`] when cancellation is observed or the
/// document/mutation path fails.
pub fn import_document_with_cancel(
    lmpg: &Lmpg,
    label: &str,
    doc: &Json,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<RecordRef, LmpgError> {
    if is_cancelled() {
        return Err(LmpgError::Conflict("import cancelled".into()));
    }
    let Json::Object(fields) = doc else {
        return Err(LmpgError::Conflict("document must be an object".into()));
    };

    let mut scalars: Vec<(String, FieldChange)> = Vec::new();
    let mut children: Vec<(String, Vec<Json>)> = Vec::new();
    for (k, v) in fields {
        match v {
            Json::Object(_) => children.push((k.clone(), vec![v.clone()])),
            Json::Array(items) if items.iter().any(serde_json::Value::is_object) => {
                children.push((k.clone(), items.clone()));
            }
            other => scalars.push((
                k.clone(),
                if other.is_null() {
                    FieldChange::ExplicitNull
                } else {
                    FieldChange::Set(other.clone())
                },
            )),
        }
    }

    if is_cancelled() {
        return Err(LmpgError::Conflict("import cancelled".into()));
    }
    let record_ref = lmpg.create_record(label, None, std::mem::take(&mut scalars))?;

    for (key, docs) in &children {
        let rel_type = child_label(label, key);
        for d in docs {
            if is_cancelled() {
                return Err(LmpgError::Conflict("import cancelled".into()));
            }
            let child = import_document_with_cancel(lmpg, &rel_type, d, is_cancelled)?;
            if is_cancelled() {
                return Err(LmpgError::Conflict("import cancelled".into()));
            }
            lmpg.create_relationship(&child_label(label, key), record_ref, child, vec![])?;
        }
    }
    Ok(record_ref)
}

/// Transactional form of [`import_document`]. All records, child records and
/// relationships are staged in the supplied accumulator and become visible
/// only when the caller commits it.
///
/// # Errors
/// Returns [`LmpgError`] when planning or authorization fails.
pub fn import_document_tx(
    lmpg: &Lmpg,
    label: &str,
    doc: &Json,
    tx: &mut rushdb_storage::TxAccumulator,
) -> Result<RecordRef, LmpgError> {
    import_document_tx_with_cancel(lmpg, label, doc, tx, &|| false)
}

/// Transactional document import with cooperative cancellation checks.
///
/// All mutations remain in `tx`; cancellation therefore discards the whole
/// staged graph instead of leaving a partially imported root or child.
///
/// # Errors
/// Returns [`LmpgError::Conflict`] when cancellation is observed or staging
/// fails.
pub fn import_document_tx_with_cancel(
    lmpg: &Lmpg,
    label: &str,
    doc: &Json,
    tx: &mut rushdb_storage::TxAccumulator,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<RecordRef, LmpgError> {
    if is_cancelled() {
        return Err(LmpgError::Conflict("import cancelled".into()));
    }
    let Json::Object(fields) = doc else {
        return Err(LmpgError::Conflict("document must be an object".into()));
    };
    let mut scalars = Vec::new();
    let mut children = Vec::new();
    for (k, v) in fields {
        match v {
            Json::Object(_) => children.push((k.clone(), vec![v.clone()])),
            Json::Array(items) if items.iter().any(serde_json::Value::is_object) => {
                children.push((k.clone(), items.clone()));
            }
            other => scalars.push((
                k.clone(),
                if other.is_null() {
                    FieldChange::ExplicitNull
                } else {
                    FieldChange::Set(other.clone())
                },
            )),
        }
    }
    if is_cancelled() {
        return Err(LmpgError::Conflict("import cancelled".into()));
    }
    let record_ref = lmpg.create_record_tx(label, None, scalars, Some(tx))?;
    for (key, docs) in children {
        let rel_type = child_label(label, &key);
        for child_doc in docs {
            if is_cancelled() {
                return Err(LmpgError::Conflict("import cancelled".into()));
            }
            let child =
                import_document_tx_with_cancel(lmpg, &rel_type, &child_doc, tx, is_cancelled)?;
            if is_cancelled() {
                return Err(LmpgError::Conflict("import cancelled".into()));
            }
            lmpg.create_relationship_tx(&rel_type, record_ref, child, vec![], Some(tx))?;
        }
    }
    Ok(record_ref)
}

/// Derive the child label for a nested key: `{parent}/{KEY}`, where non
/// alphanumeric characters in `key` become `_` and letters are uppercased.
///
/// # Examples
///
/// ```
/// use rushdb_lmpg::import::child_label;
///
/// assert_eq!(child_label("Person", "first name"), "Person/FIRST_NAME");
/// ```
#[must_use]
pub fn child_label(parent: &str, key: &str) -> String {
    let mut out = String::new();
    for c in key.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c.to_ascii_uppercase());
        } else {
            out.push('_');
        }
    }
    format!("{parent}/{out}")
}

/// Streaming line-delimited importers (document 21 slice): the Rust engine
/// decodes bytes, normalizes and lowers documents through the same
/// mutation path; source framing never changes graph shape or IDs.
///
/// # Errors
/// Returns [`LmpgError::Conflict`] if a line cannot be read or parsed, or any
/// document write fails.
pub fn import_jsonl(
    lmpg: &Lmpg,
    label: &str,
    input: impl std::io::Read,
) -> Result<Vec<RecordRef>, LmpgError> {
    use std::io::BufRead;
    let reader = std::io::BufReader::new(input);
    let mut out = Vec::new();
    for (line_no, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(l) => l.trim().to_string(),
            Err(e) => return Err(LmpgError::Conflict(format!("read error at {line_no}: {e}"))),
        };
        if line.is_empty() {
            continue;
        }
        let doc: Json = serde_json::from_str(&line)
            .map_err(|e| LmpgError::Conflict(format!("bad json on line {line_no}: {e}")))?;
        out.push(import_document(lmpg, label, &doc)?);
    }
    Ok(out)
}

/// Alias of [`import_jsonl`] for newline-delimited JSON input.
///
/// # Errors
/// Returns [`LmpgError::Conflict`] if a line cannot be read or parsed, or any
/// document write fails.
pub fn import_ndjson(
    lmpg: &Lmpg,
    label: &str,
    input: impl std::io::Read,
) -> Result<Vec<RecordRef>, LmpgError> {
    import_jsonl(lmpg, label, input)
}

/// Merge/upsert resolution (document 21): when a document carries `__id`
/// the ingestion path resolves it against existing identity instead of
/// minting a new one; fields overwrite via the normal delete+put flow.
///
/// # Errors
/// Returns [`LmpgError::Conflict`] if the document is not an object, `__id`
/// is malformed, or the read/write fails.
pub fn upsert_document(lmpg: &Lmpg, label: &str, doc: &Json) -> Result<RecordRef, LmpgError> {
    let Json::Object(obj) = doc else {
        return Err(LmpgError::Conflict("document must be an object".into()));
    };

    let Some(id_val) = obj.get("__id").and_then(|v| v.as_str()) else {
        return import_document(lmpg, label, doc);
    };
    let bytes = decode_simple_hex(id_val)
        .ok_or_else(|| LmpgError::Conflict("__id is not a 32-char hex string".into()))?;

    let snap = lmpg
        .store
        .read_view()
        .map_err(|e| LmpgError::Conflict(e.to_string()))?;
    let existing = crate::reader::try_access::record_by_id(
        &snap,
        lmpg.tenant,
        &rushdb_types::ids::RecordId(bytes),
    )
    .map_err(|e| LmpgError::Conflict(e.to_string()))?;

    let explicit = Some(bytes);
    if let Some(hdr) = existing {
        let mut scalars: Vec<(String, FieldChange)> = Vec::new();
        for (k, v) in obj {
            if k == "__id" || k == "__label" {
                continue;
            }
            scalars.push((
                k.clone(),
                if v.is_null() {
                    FieldChange::ExplicitNull
                } else {
                    FieldChange::Set(v.clone())
                },
            ));
        }
        // updates target record `hdr.ordinal` owned by public id bytes
        lmpg.update_fields(
            RecordRef {
                record_id: bytes,
                ordinal: hdr.ordinal,
            },
            std::mem::take(&mut scalars),
        )?;
        Ok(RecordRef {
            record_id: bytes,
            ordinal: hdr.ordinal,
        })
    } else {
        let mut scalars: Vec<(String, FieldChange)> = Vec::new();
        for (k, v) in obj {
            if k == "__id" || k == "__label" {
                continue;
            }
            scalars.push((
                k.clone(),
                if v.is_null() {
                    FieldChange::ExplicitNull
                } else {
                    FieldChange::Set(v.clone())
                },
            ));
        }
        lmpg.create_record(label, explicit, scalars)
    }
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Decodes a 32-character (dashes optional) hex string into 16 bytes.
#[must_use]
pub fn decode_simple_hex(hex: &str) -> Option<[u8; 16]> {
    let hex: String = hex.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        let hi = hex_nibble(hex.as_bytes()[i * 2])?;
        let lo = hex_nibble(hex.as_bytes()[i * 2 + 1])?;
        *byte = (hi << 4) | lo;
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Product-parity import (reference: platform/core entity import-export).
// BFS serializer mirrors the product's serializeBFS: nested objects and
// arrays of objects promote to child records joined by relationships; scalar
// arrays stay as ordered-many property values; `$vectors` are extracted as
// data drafts (the DB never embeds — NestJS owns model calls). mergeBy upsert
// matches existing records by property equality within a label (empty
// mergeBy = match on all keys), reusing children so parents can relink.
// ---------------------------------------------------------------------------

/// Default relationship type used when no explicit type is configured.
pub const RUSHDB_RELATION_DEFAULT: &str = "__RUSHDB__RELATION__DEFAULT__";

/// Options controlling product-parity import (type suggestion, label casing,
/// merge behavior, relationship typing).
#[expect(
    clippy::struct_excessive_bools,
    reason = "these flags mirror the reference importer's independent boolean options and are part of the public option surface"
)]
#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// Infer types (numbers, booleans, datetimes) from string values.
    pub suggest_types: bool,
    /// When set, numeric-looking strings become numbers under suggestTypes.
    pub convert_numeric_values_to_numbers: bool,
    /// Uppercase inferred labels.
    pub capitalize_labels: bool,
    /// Skip empty strings and arrays with only empty strings.
    pub skip_empty_values: bool,
    /// Default relationship type for nested joins.
    pub relationship_type: Option<String>,
    /// Per-child-key relationship types (`relationshipTypes`): nested ingest
    /// joins the parent to a child under JSON key `K` with
    /// `relationship_types[K]`, falling back to `relationship_type`. This is
    /// the only way to keep heterogeneous edge types (e.g. `HAS_NEST_*`)
    /// through one nested document; the type survives verbatim.
    pub relationship_types: std::collections::BTreeMap<String, String>,
    /// `None` = mergeBy absent (plain create); `Some(vec)` = mergeBy
    /// provided — an empty vec matches on all keys (reference uses
    /// `Array.isArray(mergeBy)`, which is true for `[]`).
    pub merge_by: Option<Vec<String>>,
    /// Merge strategy applied when upserting (`append`, `replace`, ...).
    pub merge_strategy: Option<String>,
    /// When true, importers return the created records in their result.
    pub return_result: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        ImportOptions {
            suggest_types: true,
            convert_numeric_values_to_numbers: false,
            capitalize_labels: false,
            skip_empty_values: false,
            relationship_type: None,
            relationship_types: std::collections::BTreeMap::new(),
            merge_by: None,
            merge_strategy: None,
            return_result: false,
        }
    }
}

impl ImportOptions {
    /// Validate the shape of caller-provided import options before applying
    /// defaults. Unknown fields remain forward-compatible, but a known field
    /// with the wrong JSON type must not silently become an unrelated default.
    ///
    /// # Errors
    /// Returns a message when the options value or a known field has the wrong
    /// JSON type.
    pub fn validate_json(v: &Json) -> Result<(), String> {
        let object = v
            .as_object()
            .ok_or_else(|| "import options must be an object".to_string())?;
        for key in [
            "suggestTypes",
            "convertNumericValuesToNumbers",
            "capitalizeLabels",
            "skipEmptyValues",
            "returnResult",
            "merge",
        ] {
            if object.get(key).is_some_and(|value| !value.is_boolean()) {
                return Err(format!("import option `{key}` must be boolean"));
            }
        }
        for key in ["relationshipType", "mergeStrategy"] {
            if object.get(key).is_some_and(|value| !value.is_string()) {
                return Err(format!("import option `{key}` must be string"));
            }
        }
        if let Some(value) = object.get("relationshipTypes") {
            let map = value
                .as_object()
                .ok_or_else(|| "import option `relationshipTypes` must be object".to_string())?;
            if map.values().any(|value| !value.is_string()) {
                return Err("import option `relationshipTypes` values must be strings".into());
            }
        }
        if let Some(value) = object.get("mergeBy") {
            let values = value
                .as_array()
                .ok_or_else(|| "import option `mergeBy` must be array".to_string())?;
            if values.iter().any(|value| !value.is_string()) {
                return Err("import option `mergeBy` values must be strings".into());
            }
        }
        Ok(())
    }

    /// Parses import options from a JSON object, applying reference defaults.
    #[must_use]
    pub fn from_json(v: &Json) -> Self {
        let get = |k: &str| {
            v.get(k)
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        };
        let mut opts = ImportOptions {
            suggest_types: v
                .get("suggestTypes")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true),
            convert_numeric_values_to_numbers: get("convertNumericValuesToNumbers"),
            capitalize_labels: get("capitalizeLabels"),
            skip_empty_values: get("skipEmptyValues"),
            relationship_type: v
                .get("relationshipType")
                .and_then(|x| x.as_str())
                .map(std::string::ToString::to_string),
            relationship_types: v
                .get("relationshipTypes")
                .and_then(|x| x.as_object())
                .map(|map| {
                    map.iter()
                        .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default(),
            merge_by: v.get("mergeBy").map(|x| {
                x.as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|e| e.as_str().map(std::string::ToString::to_string))
                            .collect()
                    })
                    .unwrap_or_default()
            }),
            merge_strategy: v
                .get("mergeStrategy")
                .and_then(|x| x.as_str())
                .map(std::string::ToString::to_string),
            return_result: get("returnResult"),
        };
        // `merge: true` is a shorthand for "match on all incoming keys" — the
        // same as `mergeBy: []` with the append strategy. Without this a bare
        // `merge` option was silently ignored and re-imports created duplicates.
        if let Some(merge) = v.get("merge").and_then(serde_json::Value::as_bool) {
            if merge {
                if opts.merge_by.is_none() {
                    opts.merge_by = Some(vec![]);
                }
                if opts.merge_strategy.is_none() {
                    opts.merge_strategy = Some("append".to_string());
                }
            }
        }
        opts
    }

    /// Default relationship type, or [`RUSHDB_RELATION_DEFAULT`] when unset.
    #[must_use]
    pub fn rel_type(&self) -> &str {
        self.relationship_type
            .as_deref()
            .unwrap_or(RUSHDB_RELATION_DEFAULT)
    }

    /// Relationship type for a nested child under JSON key `child_key`:
    /// `relationshipTypes[key]` when configured, else the single
    /// `relationshipType` / default.
    pub fn rel_type_for(&self, child_key: &str) -> String {
        self.relationship_types
            .get(child_key)
            .map_or_else(|| self.rel_type(), String::as_str)
            .to_string()
    }

    /// Whether the options request an upsert (a merge strategy or mergeBy).
    #[must_use]
    pub fn upsert_requested(&self) -> bool {
        self.merge_strategy.is_some() || self.merge_by.is_some()
    }
}

/// One inline vector attached to a draft record's property.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InlineVectorEntry {
    /// Property the vector is bound to.
    pub property_name: String,
    /// Vector components.
    pub vector: Vec<f32>,
    /// Optional similarity function name.
    pub similarity_function: Option<String>,
    /// Index/space name; see `rushdb_protocol::VectorQuery::index`. With
    /// `resources.require-vector-index-name` off, an absent name matches the
    /// scope's indexes by `(label, property, dimensions, similarity)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
}

/// A record draft produced by the BFS serializer.
#[derive(Debug, Clone)]
pub struct BfsRecord {
    /// Draft-local id, replaced by the persisted id after upsert.
    pub draft_id: [u8; 16],
    /// Record label.
    pub label: String,
    /// Planned field writes.
    pub properties: Vec<(String, FieldChange)>,
    /// Caller-chosen record id; `None` allocates one. Creating a record
    /// whose id already exists fails.
    pub explicit_id: Option<[u8; 16]>,
}

/// A relationship draft joining two record drafts.
#[derive(Debug, Clone)]
pub struct BfsRelation {
    /// Source record draft id.
    pub source: [u8; 16],
    /// Target record draft id.
    pub target: [u8; 16],
    /// Relationship type.
    pub rel_type: String,
}

/// A draft record carrying inline vectors extracted from `$vectors`.
#[derive(Debug, Clone)]
pub struct VectorDraft {
    /// Draft-local id.
    pub draft_id: [u8; 16],
    /// Record label.
    pub label: String,
    /// Inline vector entries.
    pub entries: Vec<InlineVectorEntry>,
}

/// The complete plan produced by the BFS serializer.
#[derive(Debug, Clone, Default)]
pub struct BfsPlan {
    /// Record drafts.
    pub records: Vec<BfsRecord>,
    /// Relationship drafts.
    pub relations: Vec<BfsRelation>,
    /// Vector drafts.
    pub vector_drafts: Vec<VectorDraft>,
    /// Validation failures encountered while extracting reserved fields.
    /// Import rejects a plan containing any error instead of treating a
    /// malformed reserved value as if it were absent.
    #[doc(hidden)]
    pub validation_errors: Vec<String>,
}

/// Mapping from a draft id to the persisted record id after an upsert.
pub struct UpsertIdMap {
    /// Draft-local id.
    pub draft_id: [u8; 16],
    /// Id of the matching persisted record.
    pub persisted_id: [u8; 16],
}

fn is_primitive(v: &Json) -> bool {
    !(v.is_object() || v.is_array() && v.as_array().is_some_and(|a| a.iter().any(Json::is_object)))
}

fn is_numeric_str(s: &str) -> bool {
    !s.is_empty() && s.parse::<f64>().is_ok()
}

/// JS `typeof`-like classification (null => "object") used to detect mixed
/// arrays, mirroring the reference `arrayIsConsistent`.
fn json_js_type(v: &Json) -> &'static str {
    match v {
        Json::Null | Json::Array(_) | Json::Object(_) => "object",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
    }
}

/// `String(item)` equivalent used when a mixed/non-scalar array is coerced to
/// a string array (reference `value.map(String)`): arrays join elements with
/// ",", plain objects become "[object Object]", scalars stringify normally.
fn json_to_js_string(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        Json::Null => "null".to_string(),
        Json::Bool(b) => b.to_string(),
        Json::Number(n) => n.to_string(),
        Json::Array(items) => items
            .iter()
            .map(json_to_js_string)
            .collect::<Vec<_>>()
            .join(","),
        Json::Object(_) => "[object Object]".to_string(),
    }
}

/// Scalar coercion mirroring the product's parsePrimitive under
/// `TImportOptions`. ISO-8601 / date-only strings become typed datetime writes
/// (epoch ms).
#[must_use]
pub fn coerce_change(value: &Json, options: &ImportOptions) -> Option<FieldChange> {
    // Product parity: an array whose elements are not all the same JS type is
    // coerced to a string array instead of failing (reference processArrayValue:
    // `isInconsistentArray || !suggestTypes ? value.map(String) : value`).
    if value.is_null() {
        // explicit Null (document 22): a live null occurrence, never an
        // untypeable `Set(null)` that the planner would reject
        return Some(FieldChange::ExplicitNull);
    }
    if let Json::Array(items) = value {
        if items.is_empty() {
            // Product parity: a genuinely empty array is preserved as `[]`
            // (the reference stores `property.value = []`), never collapsed to
            // null. The write path persists a payload-free EmptyArray marker
            // occurrence so reconstruction emits a real `[]`.
            return Some(FieldChange::Set(Json::Array(vec![])));
        }
        let is_scalar = |i: &Json| matches!(i, Json::Bool(_) | Json::Number(_) | Json::String(_));
        // pass-through: all-null arrays (kept as explicit-null elements) and
        // uniform scalar arrays (same JS type, no nulls/objects/nested arrays)
        let all_null = items.iter().all(serde_json::Value::is_null);
        let uniform_scalars = items.iter().all(is_scalar)
            && items
                .iter()
                .map(|i| json_js_type(i))
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 1;
        if !(all_null || uniform_scalars) {
            // mixed scalar types, nulls mixed with scalars, or non-scalar
            // elements (objects / nested arrays): coerce to strings — the
            // occurrence model is scalar-only and never rejects (product parity)
            let coerced: Vec<Json> = items
                .iter()
                .map(|i| Json::String(json_to_js_string(i)))
                .collect();
            return Some(FieldChange::Set(Json::Array(coerced)));
        }
    }
    if options.skip_empty_values {
        match value {
            Json::String(s) if s.is_empty() => return None,
            Json::Array(items) => {
                let stripped: Vec<Json> = items
                    .iter()
                    .filter(|i| !(i.is_string() && i.as_str().is_some_and(str::is_empty)))
                    .cloned()
                    .collect();
                if stripped.is_empty() {
                    return None;
                }
                return Some(FieldChange::Set(Json::Array(stripped)));
            }
            _ => {}
        }
    }
    if options.suggest_types {
        if let Json::String(s) = value {
            if let Some(ms) = rushdb_types::datetime::parse_datetime(s) {
                return Some(FieldChange::SetDatetime(ms));
            }
            if options.convert_numeric_values_to_numbers && is_numeric_str(s) {
                if let Ok(i) = s.parse::<i64>() {
                    return Some(FieldChange::Set(json!(i)));
                }
                if !s.contains('.')
                    && !s.contains('e')
                    && !s.contains('E')
                    && s.parse::<u64>().is_ok()
                {
                    return Some(FieldChange::Set(value.clone()));
                }
                if let Ok(f) = s.parse::<f64>() {
                    return Some(FieldChange::Set(
                        serde_json::Number::from_f64(f).map_or(Json::Null, Json::Number),
                    ));
                }
            }
            return Some(FieldChange::Set(value.clone()));
        }
        return Some(FieldChange::Set(value.clone()));
    }
    // Inference off: every value is stored as text, element by element for
    // an array (reference parity: `value.map(String)`).
    match value {
        Json::String(_) => Some(FieldChange::Set(value.clone())),
        Json::Array(items) => Some(FieldChange::Set(Json::Array(
            items
                .iter()
                .map(|item| match item {
                    Json::Null => Json::Null,
                    other => Json::String(json_to_js_string(other)),
                })
                .collect(),
        ))),
        other => Some(FieldChange::Set(Json::String(json_to_js_string(other)))),
    }
}

/// The JSON value of a `FieldChange` (for key matching against stored records).
#[must_use]
pub fn change_value(c: &FieldChange) -> Json {
    match c {
        FieldChange::Set(v) => v.clone(),
        FieldChange::SetDatetime(ms) => json!(ms),
        FieldChange::ExplicitNull | FieldChange::Delete => Json::Null,
    }
}

fn capitalize(s: &str) -> String {
    s.to_uppercase()
}

struct QueueItem {
    key: String,
    value: Json,
    parent_id: Option<[u8; 16]>,
    skip: bool,
    /// Relationship type joining this item to its parent (per-child-key);
    /// `None` for roots.
    rel_type: Option<String>,
}

/// Product-parity BFS serializer (reference serializeBFS).
#[expect(
    clippy::too_many_lines,
    reason = "the BFS walk must stay in one function so queue ordering and child-parent linkage remain visibly deterministic"
)]
#[must_use]
pub fn serialize_bfs(data: &Json, label: &str, options: &ImportOptions) -> BfsPlan {
    let mut plan = BfsPlan::default();
    let mut queue: Vec<QueueItem> = Vec::new();

    if let Some(arr) = data.as_array() {
        if !arr.is_empty() {
            for value in arr {
                queue.push(QueueItem {
                    key: if options.capitalize_labels {
                        capitalize(label)
                    } else {
                        label.to_string()
                    },
                    value: value.clone(),
                    parent_id: None,
                    skip: false,
                    rel_type: None,
                });
            }
        }
    } else {
        let has_explicit_label = !label.trim().is_empty();
        let obj = data.as_object().cloned().unwrap_or_default();
        let has_primitive_at_root = obj.iter().any(|(_, v)| is_primitive(v));
        let complex_children_count = obj
            .iter()
            .filter(|(_, v)| {
                v.is_object()
                    || (v.is_array() && v.as_array().is_some_and(|a| a.iter().any(Json::is_object)))
            })
            .count();
        let skip = !has_explicit_label || !(has_primitive_at_root || complex_children_count > 1);
        queue.push(QueueItem {
            key: if options.capitalize_labels {
                capitalize(label)
            } else {
                label.to_string()
            },
            value: data.clone(),
            parent_id: None,
            skip,
            rel_type: None,
        });
    }

    while !queue.is_empty() {
        let current = queue.remove(0);
        let mut record_draft = BfsRecord {
            draft_id: *uuid::Uuid::now_v7().as_bytes(),
            label: current.key.clone(),
            properties: Vec::new(),
            explicit_id: None,
        };

        let mut value_obj = match current.value {
            Json::Object(o) => o,
            _ => serde_json::Map::new(),
        };

        // `__id` names the record itself (the identity every read returns),
        // never a stored property: a create uses it as the new record's id
        // and an upsert by `__id` finds the record by id.
        if let Some(id) = value_obj.remove(ID_KEY) {
            match id
                .as_str()
                .and_then(|text| uuid::Uuid::parse_str(text).ok())
            {
                Some(id) => record_draft.explicit_id = Some(*id.as_bytes()),
                None => plan.validation_errors.push(format!(
                    "invalid __id {id}: a record id must be a UUID (32 hex digits, dashes optional)"
                )),
            }
        }

        let raw_vectors: Vec<InlineVectorEntry> = match value_obj.remove("$vectors") {
            Some(value) => match serde_json::from_value::<Vec<InlineVectorEntry>>(value) {
                Ok(entries) => entries,
                Err(error) => {
                    plan.validation_errors
                        .push(format!("invalid $vectors field: {error}"));
                    Vec::new()
                }
            },
            None => Vec::new(),
        };

        if !current.skip {
            if let Some(parent) = current.parent_id {
                plan.relations.push(BfsRelation {
                    source: parent,
                    target: record_draft.draft_id,
                    rel_type: current
                        .rel_type
                        .clone()
                        .unwrap_or_else(|| options.rel_type().to_string()),
                });
            }
            if !raw_vectors.is_empty() {
                plan.vector_drafts.push(VectorDraft {
                    draft_id: record_draft.draft_id,
                    label: record_draft.label.clone(),
                    entries: raw_vectors,
                });
            }
        }

        let child_parent_id = if current.skip {
            current.parent_id
        } else {
            Some(record_draft.draft_id)
        };

        for (key, value) in value_obj {
            if value.is_object() {
                let rel_type = Some(options.rel_type_for(&key));
                queue.push(QueueItem {
                    key: if options.capitalize_labels {
                        capitalize(&key)
                    } else {
                        key.clone()
                    },
                    value,
                    parent_id: child_parent_id,
                    skip: false,
                    rel_type,
                });
            } else if value.is_array()
                && value
                    .as_array()
                    .is_some_and(|a| a.iter().any(Json::is_object))
            {
                let rel_type = Some(options.rel_type_for(&key));
                for val in value.as_array().cloned().unwrap_or_default() {
                    queue.push(QueueItem {
                        key: if options.capitalize_labels {
                            capitalize(&key)
                        } else {
                            key.clone()
                        },
                        value: val,
                        parent_id: child_parent_id,
                        skip: false,
                        rel_type: rel_type.clone(),
                    });
                }
            } else if let Some(change) = coerce_change(&value, options) {
                record_draft.properties.push((key, change));
            }
        }

        if !current.skip {
            plan.records.push(record_draft);
        }
    }

    plan
}

/// The records a label-less upsert could update: per label, the record whose
/// merge keys (the `mergeBy` list, or every given property) equal
/// `properties`. The caller updates a single match, refuses several (it
/// cannot tell which one was meant) and never creates on none, because the
/// engine does not invent a label.
///
/// # Errors
/// Returns [`LmpgError::Conflict`] when the lookup cannot read the store.
pub fn find_by_keys_any_label(
    lmpg: &Lmpg,
    properties: &[(String, FieldChange)],
    options: &ImportOptions,
) -> Result<Vec<(String, RecordRef)>, LmpgError> {
    check_merge_keys(options)?;
    // The update that follows writes these properties; a reserved name is
    // refused here, before the lookup, so it is never reported as no match.
    if let Some((name, _)) = properties
        .iter()
        .find(|(name, _)| rushdb_types::value::is_reserved_name(&normalize_name(name)))
    {
        return Err(LmpgError::Plan(crate::plan::PlanError::ReservedName(
            name.clone(),
        )));
    }
    let keys = match &options.merge_by {
        Some(keys) if !keys.is_empty() => keys.clone(),
        _ => properties.iter().map(|(k, _)| k.clone()).collect(),
    };
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let snap = merge_snapshot(lmpg)?;
    let labels = crate::reader::try_access::labels(&snap, lmpg.tenant)
        .map_err(|error| LmpgError::Conflict(format!("merge lookup labels failed: {error}")))?;
    let mut index = MergeIndex::new(lmpg);
    let mut found = Vec::new();
    for label in labels {
        if let Some(record) =
            find_record_by_keys(lmpg, &snap, &mut index, &label, (&keys, None), properties)?
        {
            found.push((label, record));
        }
    }
    Ok(found)
}

fn merge_snapshot(lmpg: &Lmpg) -> Result<rushdb_storage::ReadView, LmpgError> {
    lmpg.store
        .read_view()
        .map_err(|error| LmpgError::Conflict(format!("merge lookup read failed: {error}")))
}

fn fields_from_props(props: &[(String, FieldChange)]) -> Vec<(String, FieldChange)> {
    props.to_vec()
}

fn find_record_by_keys(
    lmpg: &Lmpg,
    snap: &rushdb_storage::ReadView,
    index: &mut MergeIndex,
    label: &str,
    (keys, explicit_id): (&[String], Option<[u8; 16]>),
    props: &[(String, FieldChange)],
) -> Result<Option<RecordRef>, LmpgError> {
    if keys.iter().any(|key| normalize_name(key) == ID_KEY) {
        return find_record_by_id(lmpg, snap, label, (keys, explicit_id), props);
    }
    let Some(candidates) = index_candidates(snap, index, lmpg.tenant, label, keys, props)? else {
        return scan_record_by_keys(snap, lmpg.tenant, label, keys, props);
    };
    for ord in candidates {
        let Some(hdr) = crate::reader::try_access::record_by_ord(snap, lmpg.tenant, ord)
            .map_err(|error| LmpgError::Conflict(format!("merge lookup record failed: {error}")))?
        else {
            continue;
        };
        // Candidates come from the label's presence rows; re-check the label
        // against the header read at the same snapshot.
        if hdr.label != label {
            continue;
        }
        let Some(rec) =
            crate::reconstruct::reconstruct_record(snap, lmpg.tenant, ord).map_err(|error| {
                LmpgError::Conflict(format!("merge lookup reconstruction failed: {error}"))
            })?
        else {
            continue;
        };
        if record_matches_keys(&rec, keys, props) {
            return Ok(Some(RecordRef {
                record_id: hdr.record_id,
                ordinal: ord,
            }));
        }
    }
    Ok(None)
}

/// An upsert keyed on `__id`: one id lookup instead of a label walk. The
/// record matches when it exists under `label` and its other keys are equal;
/// a draft without an `__id` cannot match.
fn find_record_by_id(
    lmpg: &Lmpg,
    snap: &rushdb_storage::ReadView,
    label: &str,
    (keys, explicit_id): (&[String], Option<[u8; 16]>),
    props: &[(String, FieldChange)],
) -> Result<Option<RecordRef>, LmpgError> {
    let Some(id) = explicit_id else {
        return Ok(None);
    };
    let Some(hdr) = crate::reader::try_access::record_by_id(
        snap,
        lmpg.tenant,
        &rushdb_types::ids::RecordId(id),
    )
    .map_err(|error| LmpgError::Conflict(format!("merge id lookup failed: {error}")))?
    else {
        return Ok(None);
    };
    if hdr.label != label {
        return Ok(None);
    }
    let others: Vec<String> = keys
        .iter()
        .filter(|key| normalize_name(key) != ID_KEY)
        .cloned()
        .collect();
    if !others.is_empty() {
        let rec = crate::reconstruct::reconstruct_record(snap, lmpg.tenant, hdr.ordinal).map_err(
            |error| LmpgError::Conflict(format!("merge lookup reconstruction failed: {error}")),
        )?;
        if !rec.is_some_and(|rec| record_matches_keys(&rec, &others, props)) {
            return Ok(None);
        }
    }
    Ok(Some(RecordRef {
        record_id: id,
        ordinal: hdr.ordinal,
    }))
}

/// Candidate ordinals for every equality-matchable merge key: the
/// intersection of the per-key owner sets from the [`MergeIndex`]. `None`
/// means no key was matchable, so the caller must fall back to the label
/// scan; `Some(empty)` is a definitive non-match.
fn index_candidates(
    snap: &impl TryRead,
    index: &mut MergeIndex,
    tenant: TenantId,
    label: &str,
    keys: &[String],
    props: &[(String, FieldChange)],
) -> Result<Option<BTreeSet<u64>>, LmpgError> {
    let mut acc: Option<BTreeSet<u64>> = None;
    let mut indexed = false;

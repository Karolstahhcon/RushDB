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

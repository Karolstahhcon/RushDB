//! Native `RushDB` query-document parser.
//!
//! Accepts the database query-document shape and lowers it onto the
//! shared `Expr`/`RelatedExpr` execution model:
//!
//! ```json
//! { "labels": ["..."], "where": { "status": {"$in": [...]} },
//!   "select": {...}, "groupBy": [...], "orderBy": {...}, "limit": 10, "skip": 0 }
//! ```
//!
//! `where` supports flat-key implicit `$and`, `$eq/$ne/$gt/$gte/$lt/$lte/
//! $in/$nin/$contains/$startsWith/$endsWith/$exists/$type`, logical
//! `$and/$or/$nor/$xor/$not`, `$id`, label-key relationship blocks with
//! `$alias`, `$relation` (type + direction + `hops`), `$cycle`, and nested
//! multi-level blocks.

use rushdb_types::value::ID_FIELD;
use serde_json::Value as Json;

use crate::{AggregateRequest, Direction, Expr, FieldPred, Pagination, RelatedExpr, SearchRequest};

/// Errors produced while lowering a query document.
#[derive(Debug, thiserror::Error)]
pub enum QueryDocumentError {
    /// The document is malformed for a recognized construct.
    #[error("invalid where clause: {0}")]
    Parse(String),
    /// The document uses a construct this parser does not support.
    #[error("unsupported where construct: {0}")]
    Unsupported(String),
}

const OPS: &[&str] = &[
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$in",
    "$nin",
    "$contains",
    "$startsWith",
    "$endsWith",
    "$exists",
    "$type",
    "$and",
    "$or",
    "$nor",
    "$xor",
    "$not",
    "$id",
    "$relation",
    "$alias",
    "$cycle",
    "$year",
    "$month",
    "$day",
    "$hour",
    "$minute",
    "$second",
    "$millisecond",
    "$microsecond",
    "$nanosecond",
];

fn is_op_key(k: &str) -> bool {
    OPS.contains(&k)
}

/// A `$`-prefixed key that is not an operator is a typo (`$regx`), never a
/// label or field name: refuse it instead of matching nothing.
fn reject_unknown_operators(key: &str, value: &Json) -> Result<(), QueryDocumentError> {
    let unknown = std::iter::once(key)
        .chain(
            value
                .as_object()
                .into_iter()
                .flat_map(|m| m.keys().map(String::as_str)),
        )
        .find(|k| k.starts_with('$') && !is_op_key(k));
    match unknown {
        Some(op) => Err(QueryDocumentError::Parse(format!(
            "unknown operator `{op}`"
        ))),
        None => Ok(()),
    }
}

fn lit_json(v: &Json) -> Json {
    v.clone()
}

/// Parse a documented `where` object into a root `Expr` (with `Related`
/// nodes for traversal blocks). Flat keys become implicit `$and` conjuncts.
///
/// # Errors
///
/// Returns [`QueryDocumentError::Parse`] when the clause shape is invalid and
/// [`QueryDocumentError::Unsupported`] for unsupported operator placements.
pub fn parse_where(obj: &Json) -> Result<Expr, QueryDocumentError> {
    let map = obj
        .as_object()
        .ok_or_else(|| QueryDocumentError::Parse("where must be an object".into()))?;
    let mut parts: Vec<Expr> = Vec::new();
    for (k, v) in map {
        reject_unknown_operators(k, v)?;
        if k == "$and" {
            parts.push(logical(k, v, "and")?);
        } else if k == "$or" {
            parts.push(logical(k, v, "or")?);
        } else if k == "$nor" {
            parts.push(logical(k, v, "nor")?);
        } else if k == "$xor" {
            parts.push(logical(k, v, "xor")?);
        } else if k == "$not" {
            parts.push(Expr::Not(Box::new(parse_where(v)?)));
        } else if k == "$id" {
            parts.push(parse_id(v)?);
        } else if k == "$cycle" {
            parts.push(Expr::Related(parse_cycle(v)?));
        } else if is_op_key(k) {
            return Err(QueryDocumentError::Unsupported(format!(
                "root-level operator key `{k}` (operators apply to fields)"
            )));
        } else if v.is_object() && looks_like_related_block(v) {
            // label-key relationship block (with $relation/$alias/$cycle)
            parts.push(Expr::Related(parse_related_block(k, v)?));
        } else if v.is_object() && looks_like_related_logical_block(v) {
            // `{ Label: { $or: [ {field:...}, ... ] } }` — logical combo over a
            // related block (operands are field-predicate objects)
            parts.push(Expr::Related(parse_related_block(k, v)?));
        } else if v.is_object() && looks_like_operator_block(v) {
            parts.push(parse_field_ops(k, v)?);
        } else if v.is_array() && is_primitive_array(v) {
            // array of primitives under a field key: element-match ($in)
            // semantics — `{ tags: ["a", "b"] }` matches any element, the same
            // way the baseline solution wraps single values into IN predicates.
            parts.push(Expr::Field(FieldPred::In(
                norm(k),
                v.as_array().cloned().unwrap_or_default(),
            )));
        } else if v.is_object() || v.is_array() {
            // label-key relationship block (or array of blocks under a label)
            parts.push(Expr::Related(parse_related_block(k, v)?));
        } else {
            // flat equality: { field: value }
            parts.push(Expr::Field(FieldPred::Eq(norm(k), lit_json(v))));
        }
    }
    if parts.is_empty() {
        return Ok(Expr::And(vec![]));
    }
    Ok(Expr::And(parts))
}

fn logical(_k: &str, v: &Json, op: &str) -> Result<Expr, QueryDocumentError> {
    let arr = v
        .as_array()
        .ok_or_else(|| QueryDocumentError::Parse(format!("${op} requires an array")))?;
    let items: Result<Vec<Expr>, _> = arr.iter().map(parse_where).collect();
    let items = items?;
    Ok(match op {
        "or" => Expr::Or(items),
        "nor" => Expr::Nor(items),
        "xor" => {
            if items.len() != 2 {
                return Err(QueryDocumentError::Parse(
                    "$xor requires exactly two conditions".into(),
                ));
            }
            Expr::Xor(Box::new(items[0].clone()), Box::new(items[1].clone()))
        }
        _ => Expr::And(items),
    })
}

/// A field operator block: `{ field: { $gte: 1, $lt: 5 } }` is an implicit
/// $and over each operator; multiple operators compose.
fn parse_field_ops(field: &str, v: &Json) -> Result<Expr, QueryDocumentError> {
    let map = v
        .as_object()
        .ok_or_else(|| QueryDocumentError::Parse("operator block must be an object".into()))?;
    let mut parts = Vec::new();
    for (op, value) in map {
        if op == "$or" {
            parts.push(Expr::Or(parse_field_conditions(field, value)?));
        } else if op == "$and" {
            parts.push(Expr::And(parse_field_conditions(field, value)?));
        } else if op == "$nor" {
            parts.push(Expr::Nor(parse_field_conditions(field, value)?));
        } else if op == "$xor" {
            let items = parse_field_conditions(field, value)?;
            if items.len() != 2 {
                return Err(QueryDocumentError::Parse(
                    "$xor requires exactly two conditions".into(),
                ));
            }
            parts.push(Expr::Xor(
                Box::new(items[0].clone()),
                Box::new(items[1].clone()),
            ));
        } else if op == "$not" {
            parts.push(Expr::Not(Box::new(parse_field_or_logical(field, value)?)));
        } else {
            parts.push(parse_one_op(field, op, value)?);
        }
    }
    Ok(if parts.is_empty() {
        Expr::And(vec![])
    } else if parts.len() == 1 {
        parts.remove(0)
    } else {
        Expr::And(parts)
    })
}

/// Logical conditions scoped to a field: each array element is either an
/// operator-only object (applied to `field`) or a full condition object.
fn parse_field_conditions(field: &str, v: &Json) -> Result<Vec<Expr>, QueryDocumentError> {
    let arr = v
        .as_array()
        .ok_or_else(|| QueryDocumentError::Parse("logical operator requires an array".into()))?;
    let mut out = Vec::new();
    for item in arr {
        if let Some(m) = item.as_object() {
            if m.keys().all(|k| is_op_key(k)) {
                out.push(parse_field_ops(field, item)?);
            } else {
                out.push(parse_where(item)?);
            }
        } else {
            return Err(QueryDocumentError::Parse(
                "condition must be an object".into(),
            ));
        }
    }
    Ok(out)
}

fn parse_field_or_logical(field: &str, v: &Json) -> Result<Expr, QueryDocumentError> {
    if let Some(m) = v.as_object() {
        if m.keys().all(|k| is_op_key(k)) {
            return parse_field_ops(field, v);
        }
    }
    parse_where(v)
}

fn looks_like_related_logical_block(v: &Json) -> bool {
    let Some(m) = v.as_object() else { return false };
    if !m
        .keys()
        .all(|k| matches!(k.as_str(), "$and" | "$or" | "$nor" | "$xor" | "$not"))
    {
        return false;
    }
    // related-block only when an operand is a field-predicate object (has a
    // non-operator key); a `$or` of pure operator blocks is field-value logic
    for val in m.values() {
        if let Some(arr) = val.as_array() {
            if arr.iter().any(|e| {
                e.as_object()
                    .is_some_and(|o| o.keys().any(|k| !is_op_key(k)))
            }) {
                return true;
            }
        } else if let Some(o) = val.as_object() {
            if o.keys().any(|k| !is_op_key(k)) {
                return true;
            }
        }
    }
    false
}

fn looks_like_operator_block(v: &Json) -> bool {
    v.as_object()
        .is_some_and(|m| m.keys().any(|k| is_op_key(k) && !is_relation_key(k)))
}

fn is_relation_key(k: &str) -> bool {
    matches!(k, "$relation" | "$alias" | "$cycle" | "$id")
}

/// True when `v` is a JSON array whose elements are all scalars (no nested
/// objects/arrays). Used to disambiguate `{ field: [v1, v2] }` element-match
/// semantics from a label-key array of relationship blocks.
fn is_primitive_array(v: &Json) -> bool {
    v.as_array().is_some_and(|items| {
        items
            .iter()
            .all(|item| !item.is_object() && !item.is_array())
    })
}

fn looks_like_related_block(v: &Json) -> bool {
    v.as_object()
        .is_some_and(|m| m.keys().any(|k| is_relation_key(k)))
}

fn parse_one_op(field: &str, op: &str, value: &Json) -> Result<Expr, QueryDocumentError> {
    let f = norm(field);
    match op {
        "$eq" => Ok(Expr::Field(FieldPred::Eq(f, dt_lit(value)))),
        "$ne" => Ok(Expr::Field(FieldPred::Ne(f, dt_lit(value)))),
        "$gt" => Ok(Expr::Field(FieldPred::Gt(f, dt_num(value)?))),
        "$gte" => Ok(Expr::Field(FieldPred::Gte(f, dt_num(value)?))),
        "$lt" => Ok(Expr::Field(FieldPred::Lt(f, dt_num(value)?))),
        "$lte" => Ok(Expr::Field(FieldPred::Lte(f, dt_num(value)?))),
        "$in" => Ok(Expr::Field(FieldPred::In(f, array_of(value)?))),
        "$nin" => Ok(Expr::Field(FieldPred::Nin(f, array_of(value)?))),
        "$contains" => Ok(Expr::Field(FieldPred::Contains(f, str_of(value)?))),
        "$startsWith" => Ok(Expr::Field(FieldPred::StartsWith(f, str_of(value)?))),
        "$endsWith" => Ok(Expr::Field(FieldPred::EndsWith(f, str_of(value)?))),
        "$exists" => {
            let want = value
                .as_bool()
                .ok_or_else(|| QueryDocumentError::Parse("$exists requires a boolean".into()))?;
            Ok(Expr::Field(FieldPred::Exists(f, want)))
        }
        "$type" => Ok(Expr::Field(FieldPred::Type(f, str_of(value)?))),
        "$id" => parse_id(value),
        "$year" => Ok(Expr::Field(FieldPred::DatetimeComponent {
            field: f,
            which: 0,
            value: comp_int(value)?,
        })),
        "$month" => Ok(Expr::Field(FieldPred::DatetimeComponent {
            field: f,
            which: 1,
            value: comp_int(value)?,
        })),
        "$day" => Ok(Expr::Field(FieldPred::DatetimeComponent {
            field: f,
            which: 2,
            value: comp_int(value)?,
        })),
        "$hour" => Ok(Expr::Field(FieldPred::DatetimeComponent {
            field: f,
            which: 3,
            value: comp_int(value)?,
        })),
        "$minute" => Ok(Expr::Field(FieldPred::DatetimeComponent {
            field: f,
            which: 4,
            value: comp_int(value)?,
        })),
        "$second" => Ok(Expr::Field(FieldPred::DatetimeComponent {
            field: f,
            which: 5,
            value: comp_int(value)?,
        })),
        "$millisecond" => Ok(Expr::Field(FieldPred::DatetimeComponent {
            field: f,
            which: 6,
            value: comp_int(value)?,
        })),
        "$microsecond" => Ok(Expr::Field(FieldPred::DatetimeComponent {
            field: f,
            which: 7,
            value: comp_int(value)?,
        })),
        "$nanosecond" => Ok(Expr::Field(FieldPred::DatetimeComponent {
            field: f,
            which: 8,
            value: comp_int(value)?,
        })),
        other => Err(QueryDocumentError::Unsupported(other.to_string())),
    }
}

fn parse_id(v: &Json) -> Result<Expr, QueryDocumentError> {
    if let Some(s) = v.as_str() {
        return Ok(Expr::Field(FieldPred::Id(s.to_string())));
    }
    if let Some(map) = v.as_object() {
        // { $id: { $in: [...] } } form
        if let Some(arr) = map.get("$in").and_then(|x| x.as_array()) {
            let ids = arr
                .iter()
                .map(|x| {
                    x.as_str()
                        .map(|id| Expr::Field(FieldPred::Id(id.to_string())))
                        .ok_or_else(|| {
                            QueryDocumentError::Parse("$id.$in requires string ids".into())
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(Expr::Or(ids));
        }
    }
    Err(QueryDocumentError::Parse(
        "$id requires a string or $in list".into(),
    ))
}

/// `{ LABEL: { $alias, $relation, ...props, NESTED_LABEL: {...} } }`
/// `$and`/`$or`/`$nor`/`$not` over predicates on a related record. Each
/// element lowers through `parse_where` so both field predicates and nested
/// related blocks are supported.
fn parse_related_logical(op: &str, v: &Json) -> Result<Expr, QueryDocumentError> {
    let subs = |arr: &Json| -> Result<Vec<Expr>, QueryDocumentError> {
        arr.as_array()
            .ok_or_else(|| QueryDocumentError::Parse(format!("{op} requires an array")))?
            .iter()
            .map(parse_where)
            .collect()
    };
    match op {
        "$and" => Ok(Expr::And(subs(v)?)),
        "$or" => Ok(Expr::Or(subs(v)?)),
        "$nor" => Ok(Expr::Nor(subs(v)?)),
        "$xor" => {
            let arr = subs(v)?;
            if arr.len() != 2 {
                return Err(QueryDocumentError::Parse(
                    "$xor requires exactly two operands".into(),
                ));
            }
            Ok(Expr::Xor(
                Box::new(arr[0].clone()),
                Box::new(arr[1].clone()),
            ))
        }
        "$not" => Ok(Expr::Not(Box::new(parse_where(v)?))),
        _ => unreachable!(),
    }
}

fn parse_related_block(label: &str, v: &Json) -> Result<RelatedExpr, QueryDocumentError> {
    let mut rel_type: Option<String> = None;
    let mut direction = Direction::Both;
    let mut hops: Option<(u64, u64)> = None;
    let mut cycle = false;
    let mut alias = None;

    let mut field_preds: Vec<Expr> = Vec::new();
    let mut nested_related: Vec<Expr> = Vec::new();

    let items: Vec<(String, Json)> = if v.is_array() {
        // single label can map to an array of blocks; treat each as the
        // same label block (union semantics approximated by And-of-first)
        let arr = v.as_array().unwrap();
        let mut out = Vec::new();
        for item in arr {
            if let Some(m) = item.as_object() {
                out.extend(m.iter().map(|(k, val)| (k.clone(), val.clone())));
            }
        }
        out
    } else if let Some(m) = v.as_object() {
        m.iter().map(|(k, val)| (k.clone(), val.clone())).collect()
    } else {
        return Err(QueryDocumentError::Parse(
            "related block must be an object".into(),
        ));
    };

    for (k, value) in items {
        reject_unknown_operators(&k, &value)?;
        if k == "$alias" {
            alias = Some(str_of(&value)?);
        } else if k == "$id" {
            // filter the related record by its identity
            let id = value
                .as_str()
                .ok_or_else(|| QueryDocumentError::Parse("$id requires a string".into()))?;
            field_preds.push(Expr::Field(FieldPred::Id(id.to_string())));
        } else if k == "$relation" {
            if let Some(s) = value.as_str() {
                rel_type = Some(s.to_string());
            } else if let Some(m) = value.as_object() {
                if let Some(t) = m.get("type").and_then(|x| x.as_str()) {
                    rel_type = Some(t.to_string());
                }
                if let Some(d) = m.get("direction").and_then(|x| x.as_str()) {
                    direction = parse_direction(d)?;
                }
                if let Some(h) = m.get("hops") {
                    hops = Some(parse_hops(h)?);
                }
            }
        } else if k == "$cycle" {
            cycle = true;
            let c = parse_cycle(&value)?;
            rel_type = c.rel_type;
            direction = c.direction;
            hops = c.hops;
        } else if matches!(k.as_str(), "$and" | "$or" | "$nor" | "$xor" | "$not") {
            // logical combination of predicates on the related record
            field_preds.push(parse_related_logical(&k, &value)?);
        } else if is_op_key(&k) {
            return Err(QueryDocumentError::Parse(format!(
                "operator `{k}` cannot apply to the related block `{label}`; put it on a field"
            )));
        } else if value.is_object() && looks_like_operator_block(&value) {
            field_preds.push(parse_field_ops(&k, &value)?);
        } else if value.is_object() || value.is_array() {
            // nested label block → child RelatedExpr
            let child = parse_related_block(&k, &value)?;
            nested_related.push(Expr::Related(child));
        } else {
            field_preds.push(Expr::Field(FieldPred::Eq(norm(&k), lit_json(&value))));
        }
    }

    let mut filter_parts = field_preds;
    filter_parts.extend(nested_related);
    let filter = if filter_parts.is_empty() {
        None
    } else if filter_parts.len() == 1 {
        Some(Box::new(filter_parts.remove(0)))
    } else {
        Some(Box::new(Expr::And(filter_parts)))
    };

    Ok(RelatedExpr {
        target_labels: vec![label.to_string()],
        alias,
        rel_type,
        direction,
        hops,
        cycle,
        filter,
    })
}

fn parse_direction(direction: &str) -> Result<Direction, QueryDocumentError> {
    match direction {
        "in" | "incoming" => Ok(Direction::Incoming),
        "out" | "outgoing" => Ok(Direction::Outgoing),
        "both" | "any" => Ok(Direction::Both),
        other => Err(QueryDocumentError::Parse(format!(
            "unknown relationship direction `{other}` (expected in, out or both)"
        ))),
    }
}

fn parse_cycle(v: &Json) -> Result<RelatedExpr, QueryDocumentError> {
    let m = v
        .as_object()
        .ok_or_else(|| QueryDocumentError::Parse("$cycle requires an object".into()))?;
    let mut rel_type = None;
    let mut direction = Direction::Both;
    if let Some(t) = m.get("type").and_then(|x| x.as_str()) {
        rel_type = Some(t.to_string());
    }
    if let Some(d) = m.get("direction").and_then(|x| x.as_str()) {
        direction = parse_direction(d)?;
    }
    let hops = match m.get("hops") {
        Some(h) => parse_hops(h)?,
        None => {
            return Err(QueryDocumentError::Parse("$cycle requires hops".into()));
        }
    };
    if hops.0 < 2 {
        return Err(QueryDocumentError::Parse(
            "$cycle hops.min must be at least 2".into(),
        ));
    }
    Ok(RelatedExpr {
        target_labels: Vec::new(),
        alias: None,
        rel_type,
        direction,
        hops: Some(hops),
        cycle: true,
        filter: None,
    })
}

/// `hops: 3` → (3,3); `hops: {min, max}` → bounds (min defaults 1).
fn parse_hops(v: &Json) -> Result<(u64, u64), QueryDocumentError> {
    if let Some(n) = v.as_u64() {
        if n == 0 {
            return Err(QueryDocumentError::Parse("hops must be at least 1".into()));
        }
        return Ok((n, n));
    }
    let m = v
        .as_object()
        .ok_or_else(|| QueryDocumentError::Parse("hops must be int or object".into()))?;
    let min = m
        .get("min")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    let max = m
        .get("max")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(10);
    if max < min {
        return Err(QueryDocumentError::Parse("hops.max < hops.min".into()));
    }
    if min == 0 {
        return Err(QueryDocumentError::Parse(
            "hops.min must be at least 1".into(),
        ));
    }
    Ok((min, max))
}

fn comp_int(v: &Json) -> Result<i64, QueryDocumentError> {
    v.as_i64()
        .or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok()))
        .ok_or_else(|| QueryDocumentError::Parse("expected integer".into()))
}

/// Datetime-aware literal for equality: ISO strings / component objects
/// lower to epoch-ms numbers (matching the engine `DateTime` type).
/// An equality literal. ISO-8601 text stays text: it equals a string holding
/// that text and a datetime at that instant, which the index probes and the
/// evaluator both resolve. A component object names one instant.
fn dt_lit(v: &Json) -> Json {
    if let Some(m) = v.as_object() {
        if m.contains_key("$year") {
            if let Some(ms) = crate::datetime::parse_components(m) {
                return serde_json::json!(ms);
            }
        }
    }
    lit_json(v)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "epoch-millisecond bounds are compared as f64 throughout the evaluator, matching its numeric comparison semantics"
)]
fn dt_num(v: &Json) -> Result<f64, QueryDocumentError> {
    // numeric compares accept ISO strings / component objects as epoch ms
    if let Some(s) = v.as_str() {
        if let Some(ms) = crate::datetime::parse_iso_8601(s) {
            return Ok(ms as f64);
        }
    }
    if let Some(m) = v.as_object() {
        if let Some(ms) = crate::datetime::parse_components(m) {
            return Ok(ms as f64);
        }
    }
    num(v)
}

#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    reason = "the casts are paired with an i128 round-trip check; imprecise integer bounds are rejected"
)]
fn num(v: &Json) -> Result<f64, QueryDocumentError> {
    let Some(number) = v.as_number() else {
        return Err(QueryDocumentError::Parse("expected number".into()));
    };
    if let Some(i) = number.as_i64() {
        let f = i as f64;
        if (f as i128) == i128::from(i) {
            return Ok(f);
        }
        return Err(QueryDocumentError::Parse(
            "integer range bound is not exactly representable".into(),
        ));
    }
    if let Some(u) = number.as_u64() {
        let f = u as f64;
        if (f as i128) == i128::from(u) {
            return Ok(f);
        }
        return Err(QueryDocumentError::Parse(
            "unsigned integer range bound is not exactly representable".into(),
        ));
    }
    number
        .as_f64()
        .ok_or_else(|| QueryDocumentError::Parse("expected number".into()))
}

fn str_of(v: &Json) -> Result<String, QueryDocumentError> {
    v.as_str()
        .map(std::string::ToString::to_string)
        .ok_or_else(|| QueryDocumentError::Parse("expected string".into()))
}

fn array_of(v: &Json) -> Result<Vec<Json>, QueryDocumentError> {
    v.as_array()
        .cloned()
        .ok_or_else(|| QueryDocumentError::Parse("expected array".into()))
}

fn norm(s: &str) -> String {
    crate::normalize(s)
}

/// Parse a `select` object into aggregate request metrics.
/// select value forms: `"$record.field"`, `"$alias"`, `"$alias.field"`,
/// `{"$count": "*" | "$alias" | "$alias.field"}`, `{"$sum": "$record.field"}`,
/// `{"$avg": ...}`, `{"$min": ...}`, `{"$max": ...}`.
///
/// # Errors
///
/// Returns [`QueryDocumentError::Parse`] for malformed select/collect shapes
/// and [`QueryDocumentError::Unsupported`] for unknown select operators.
pub fn parse_select(
    select: &Json,
    aliases: &CollectAliasMap,
) -> Result<AggregateRequest, QueryDocumentError> {
    let map = select
        .as_object()
        .ok_or_else(|| QueryDocumentError::Parse("select must be an object".into()))?;
    let mut req = AggregateRequest::default();
    let mut internal_metric = 0usize;
    for (name, spec) in map {
        let out = name.clone();
        if let Some(ops) = spec.as_object() {
            for (op, target) in ops {
                match op.as_str() {
                    "$count" => {
                        let field = select_field(target)?;
                        req.counts.push((out.clone(), field));
                    }
                    "$sum" => {
                        if let Some(expr) = select_math_expr(target)? {
                            req.expr_sums.push((out.clone(), expr));
                        } else {
                            req.sums.push((out.clone(), select_field(target)?));
                        }
                    }
                    "$avg" => {
                        if let Some(expr) = select_math_expr(target)? {
                            req.expr_avgs.push((out.clone(), expr));
                        } else {
                            req.avg_terms.push((out.clone(), select_field(target)?));
                        }
                        if let Some(prec) =
                            ops.get("$precision").and_then(serde_json::Value::as_u64)
                        {
                            let prec = u32::try_from(prec)
                                .unwrap_or(u32::MAX)
                                .min(crate::MAX_AVG_PRECISION);
                            req.avg_precision.insert(out.clone(), prec);
                        }
                    }
                    "$min" => req.mins.push((out.clone(), select_field(target)?)),
                    "$max" => req.maxs.push((out.clone(), select_field(target)?)),
                    "$ref" => req
                        .outputs
                        .push((out.clone(), crate::OutputExpr::Ref(select_field(target)?))),
                    "$subtract" | "$add" | "$multiply" | "$divide" => {
                        let expr =
                            build_derived_math(&mut req, &out, &mut internal_metric, op, target)?;
                        req.outputs.push((out.clone(), expr));
                    }
                    "$timeBucket" => {
                        if let Some(bucket) = target.as_object() {
                            let f = bucket
                                .get("field")
                                .map(select_field)
                                .transpose()?
                                .unwrap_or_default();
                            let unit = crate::TimeUnit::from_name(
                                bucket
                                    .get("unit")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or("month"),
                            )?;
                            let size = bucket
                                .get("size")
                                .and_then(serde_json::Value::as_u64)
                                .unwrap_or(1);
                            if size == 0 {
                                return Err(QueryDocumentError::Parse(
                                    "$timeBucket `size` must be at least 1".into(),
                                ));
                            }
                            req.time_bucket = Some(crate::TimeBucketSpec {
                                field: f,
                                unit,
                                size,
                            });
                            req.time_bucket_alias = Some(out.clone());
                        } else {
                            return Err(QueryDocumentError::Parse(
                                "$timeBucket requires an object".into(),
                            ));
                        }
                    }
                    "$collect" => {
                        req.collects.push(parse_collect(&out, target, aliases)?);
                    }
                    "$precision" => {}
                    other => {
                        return Err(QueryDocumentError::Unsupported(format!(
                            "select operator `{other}`"
                        )));
                    }
                }
            }
        } else if let Some(f) = spec.as_str() {
            let field = select_field(&Json::String(f.to_string()))?;
            req.plain_fields.push((out, field));
        }
    }
    Ok(req)
}

/// Lower post-aggregation arithmetic. Inline aggregate operands are assigned
/// private aliases and materialized before the derived expression is evaluated.
fn build_derived_math(
    req: &mut AggregateRequest,
    output: &str,
    counter: &mut usize,
    op: &str,
    value: &Json,
) -> Result<crate::OutputExpr, QueryDocumentError> {
    let args = value
        .as_array()
        .ok_or_else(|| QueryDocumentError::Parse(format!("{op} requires an array")))?;
    if args.len() != 2 {
        return Err(QueryDocumentError::Parse(format!(
            "{op} expects exactly two operands"
        )));
    }
    let mut operands = Vec::with_capacity(2);
    for arg in args {
        operands.push(lower_derived_operand(req, output, counter, arg)?);
    }
    Ok(match op {
        "$multiply" => crate::OutputExpr::Multiply(
            Box::new(operands[0].clone()),
            Box::new(operands[1].clone()),
        ),
        "$divide" => crate::OutputExpr::Divide {
            dividend: Box::new(operands[0].clone()),
            divisor: Box::new(operands[1].clone()),
            precision: 8,
        },
        "$add" => {
            crate::OutputExpr::Add(Box::new(operands[0].clone()), Box::new(operands[1].clone()))
        }
        "$subtract" => crate::OutputExpr::Subtract(
            Box::new(operands[0].clone()),
            Box::new(operands[1].clone()),
        ),
        other => {
            return Err(QueryDocumentError::Unsupported(format!(
                "select math `{other}`"
            )))
        }
    })
}

fn lower_derived_operand(
    req: &mut AggregateRequest,
    output: &str,
    counter: &mut usize,
    value: &Json,
) -> Result<crate::OutputExpr, QueryDocumentError> {
    if value.is_number() {
        // Preserve the query language's exact integer-bound rule here too;
        // `Json::as_f64` would silently round integers above 2^53.
        return Ok(crate::OutputExpr::Literal(num(value)?));
    }
    if let Some(field) = value.as_str() {
        return Ok(crate::OutputExpr::Ref(select_field(&Json::String(
            field.to_string(),
        ))?));
    }
    let ops = value
        .as_object()
        .ok_or_else(|| QueryDocumentError::Parse("unsupported derived operand".into()))?;
    if let Some(reference) = ops.get("$ref") {
        return Ok(crate::OutputExpr::Ref(select_field(reference)?));
    }
    for math in ["$add", "$subtract", "$multiply", "$divide"] {
        if let Some(args) = ops.get(math) {
            return build_derived_math(req, output, counter, math, args);
        }
    }

    let alias = format!("__{output}_{}", *counter);
    *counter += 1;
    if let Some(target) = ops.get("$sum") {
        if let Some(expr) = select_math_expr(target)? {
            req.expr_sums.push((alias.clone(), expr));
        } else {
            req.sums.push((alias.clone(), select_field(target)?));
        }
    } else if let Some(target) = ops.get("$avg") {
        if let Some(expr) = select_math_expr(target)? {
            req.expr_avgs.push((alias.clone(), expr));
        } else {
            req.avg_terms.push((alias.clone(), select_field(target)?));
        }
        if let Some(precision) = ops.get("$precision").and_then(Json::as_u64) {
            let precision = u32::try_from(precision)
                .unwrap_or(u32::MAX)
                .min(crate::MAX_AVG_PRECISION);
            req.avg_precision.insert(alias.clone(), precision);
        }
    } else if let Some(target) = ops.get("$count") {
        req.counts.push((alias.clone(), select_field(target)?));
    } else if let Some(target) = ops.get("$min") {
        req.mins.push((alias.clone(), select_field(target)?));
    } else if let Some(target) = ops.get("$max") {
        req.maxs.push((alias.clone(), select_field(target)?));
    } else {
        return Err(QueryDocumentError::Parse(
            "expected aggregate or math operand".into(),
        ));
    }
    req.hidden_outputs.insert(alias.clone());
    Ok(crate::OutputExpr::Ref(alias))
}

/// Parse a nested math expression used as an aggregate target, e.g.
/// `{ $sum: { $multiply: ['$record.price', '$record.qty'] } }`.
/// Build a math expression from an operand array for a given operator.
fn build_math_from_array(op: &str, v: &Json) -> Result<crate::OutputExpr, QueryDocumentError> {
    let argv = v
        .as_array()
        .ok_or_else(|| QueryDocumentError::Parse(format!("{op} requires an array")))?;
    let operands: Result<Vec<crate::OutputExpr>, QueryDocumentError> = argv
        .iter()
        .map(|a| {
            if let Some(s) = a.as_str() {
                Ok(crate::OutputExpr::Field(select_field(&Json::String(
                    s.to_string(),
                ))?))
            } else if a.is_number() {
                // Keep nested aggregate math on the same exact numeric gate as
                // top-level derived expressions; direct `as_f64` would round
                // integers above 2^53 before planning.
                Ok(crate::OutputExpr::Literal(num(a)?))
            } else if let Some(m) = a.as_object() {
                // { $ref: 'name' } operand → derived-output reference
                if m.len() == 1 {
                    if let Some(name) = m.get("$ref").and_then(|x| x.as_str()) {
                        return Ok(crate::OutputExpr::Ref(select_field(&Json::String(
                            name.to_string(),
                        ))?));
                    }
                }
                select_math_expr(&Json::Object(m.clone()))?
                    .ok_or_else(|| QueryDocumentError::Parse("expected math expression".into()))
            } else {
                Err(QueryDocumentError::Parse("unsupported operand".into()))
            }
        })
        .collect();
    let operands = operands?;
    if operands.len() != 2 {
        return Err(QueryDocumentError::Parse(format!(
            "{op} expects exactly two operands"
        )));
    }
    Ok(match op {
        "$multiply" => crate::OutputExpr::Multiply(
            Box::new(operands[0].clone()),
            Box::new(operands[1].clone()),
        ),
        "$divide" => crate::OutputExpr::Divide {
            dividend: Box::new(operands[0].clone()),
            divisor: Box::new(operands[1].clone()),
            precision: 8,
        },
        "$add" => {
            crate::OutputExpr::Add(Box::new(operands[0].clone()), Box::new(operands[1].clone()))
        }
        "$subtract" => crate::OutputExpr::Subtract(
            Box::new(operands[0].clone()),
            Box::new(operands[1].clone()),
        ),
        other => {
            return Err(QueryDocumentError::Unsupported(format!(
                "select math `{other}`"
            )))
        }
    })
}

fn select_math_expr(v: &Json) -> Result<Option<crate::OutputExpr>, QueryDocumentError> {
    let Some(ops) = v.as_object() else {
        return Ok(None);
    };
    if ops.len() != 1 {
        return Err(QueryDocumentError::Parse(
            "math expression must have a single operator".into(),
        ));
    }
    let (op, args) = ops.iter().next().unwrap();
    args.as_array()
        .ok_or_else(|| QueryDocumentError::Parse(format!("{op} requires an array")))?;
    Ok(Some(build_math_from_array(op.as_str(), args)?))
}

fn select_field(v: &Json) -> Result<String, QueryDocumentError> {
    let s = v
        .as_str()
        .ok_or_else(|| QueryDocumentError::Parse("select target must be a string".into()))?;
    // `$record.field` and `$self.field` resolve to `field` (the current /
    // root record); `$alias.field` keeps the dotted alias path (used by
    // count/sum over related aliases).
    let s = s
        .trim_start_matches("$record.")
        .trim_start_matches("$self.")
        .trim_start_matches('$');
    if s.is_empty() {
        return Err(QueryDocumentError::Parse(
            "select target must reference a field (e.g. `$record.name` or `$self.name`)".into(),
        ));
    }
    Ok(s.to_string())
}

/// Parse an `orderBy` value (`"asc"|"desc"` or `{field:"asc"}`) into
/// `(normalized_field, descending)` pairs.
fn parse_order_by(v: Option<&Json>) -> Vec<(String, bool)> {
    match v {
        Some(v) if v.is_string() => vec![(ID_FIELD.into(), v.as_str().unwrap() == "desc")],
        Some(v) => v
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, d)| {
                        let field = if k == "id" {
                            ID_FIELD.to_string()
                        } else {
                            norm(k)
                        };
                        (field, d.as_str().unwrap_or("asc") == "desc")

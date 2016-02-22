//! Plain Grafeo (its own in-memory store, no LMPG, no `RushDB` adapter) as a
//! reference for attributing openCypher TCK failures: a scenario that fails
//! on `RushDB` but passes here fails because of `RushDB`, not Grafeo.
//!
//! Speaks JSON lines on stdin/stdout, one response per command:
//! `{"op": "reset"}` → `{}`; `{"op": "run", "statement": "...",
//! "parameters": {...}}` → `{"rows": [...]}` or `{"error": "..."}`;
//! `{"op": "snapshot"}` → `{"nodes": [...], "relationships": [...]}`.
//! Values use the protocol's JSON shapes (`_id`, `__labels`, `type`,
//! `source`, `target`, paths as `{"nodes", "edges"}`, temporal ISO text).
//! Driven by `scripts/run-opencypher-tck.py --reference`.

use std::collections::HashMap;
use std::io::{BufRead, Write};

use grafeo_common::types::{EdgeId, LogicalType, NodeId, PropertyKey, Value};
use grafeo_engine::GrafeoDB;
use serde_json::{json, Map, Value as Json};

fn value_json(db: &GrafeoDB, kind: Option<&LogicalType>, value: &Value) -> Json {
    match (kind, value) {
        (Some(LogicalType::Node), Value::Int64(id)) => {
            node_json(db, NodeId::new(u64::try_from(*id).unwrap_or_default()))
        }
        (Some(LogicalType::Edge), Value::Int64(id)) => {
            edge_json(db, EdgeId::new(u64::try_from(*id).unwrap_or_default()))
        }
        (Some(LogicalType::List(inner)), Value::List(items)) => Json::Array(
            items
                .iter()
                .map(|item| value_json(db, Some(inner), item))
                .collect(),
        ),
        (_, Value::Map(map)) => {
            let id = map.get(&PropertyKey::new("_id"));
            if let (Some(Value::Int64(id)), true) =
                (id, map.contains_key(&PropertyKey::new("_labels")))
            {
                return node_json(db, NodeId::new(u64::try_from(*id).unwrap_or_default()));
            }
            if let (Some(Value::Int64(id)), true) =
                (id, map.contains_key(&PropertyKey::new("_type")))
            {
                return edge_json(db, EdgeId::new(u64::try_from(*id).unwrap_or_default()));
            }
            Json::Object(
                map.iter()
                    .map(|(key, value)| (key.to_string(), value_json(db, None, value)))
                    .collect(),
            )
        }
        (_, Value::Path { nodes, edges }) => json!({
            "nodes": nodes.iter().map(|n| value_json(db, Some(&LogicalType::Node), n)).collect::<Vec<_>>(),
            "edges": edges.iter().map(|e| value_json(db, Some(&LogicalType::Edge), e)).collect::<Vec<_>>(),
        }),
        (_, Value::Null) => Json::Null,
        (_, Value::Bool(b)) => Json::Bool(*b),
        (_, Value::Int64(i)) => json!(i),
        (_, Value::Float64(f)) => serde_json::Number::from_f64(*f).map_or(Json::Null, Json::Number),
        (_, Value::String(s)) => Json::String(s.to_string()),
        (_, Value::List(items)) => Json::Array(
            items
                .iter()
                .map(|item| value_json(db, None, item))
                .collect(),
        ),
        (_, Value::Date(d)) => Json::String(d.to_string()),
        (_, Value::Time(t)) => Json::String(t.to_string()),
        (_, Value::Duration(d)) => Json::String(d.to_string()),
        (_, Value::ZonedDatetime(z)) => Json::String(z.to_string()),
        (_, other) => serde_json::to_value(other).unwrap_or(Json::Null),
    }
}

fn properties(map: &mut Map<String, Json>, db: &GrafeoDB, props: Vec<(String, Value)>) {
    for (key, value) in props {
        map.insert(key, value_json(db, None, &value));

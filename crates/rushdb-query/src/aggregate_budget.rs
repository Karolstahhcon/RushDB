//! Live aggregate retention and per-record reconstruction admission.
use std::cell::Cell;
use std::sync::Arc;

use rushdb_lmpg::reader::TryRead;
use rushdb_storage::StorageError;
use serde_json::Value;

use crate::{GroupBucket, QueryError, QuerySnapshot};

pub(super) fn check(limit: Option<usize>, bytes: usize) -> Result<(), QueryError> {
    if limit.is_some_and(|limit| bytes > limit) {
        return Err(QueryError::ResourceLimit(
            "aggregate working byte budget exceeded".into(),
        ));
    }
    Ok(())
}

pub(super) fn value_bytes(value: &Value) -> usize {
    let payload = match value {
        Value::String(value) => value.len(),
        Value::Array(values) => values
            .iter()
            .fold(0usize, |sum, v| sum.saturating_add(value_bytes(v))),
        Value::Object(values) => object_bytes(values),
        _ => 0,
    };
    std::mem::size_of::<Value>().saturating_add(payload)
}

pub(super) fn object_bytes(values: &serde_json::Map<String, Value>) -> usize {
    values.iter().fold(0usize, |sum, (key, value)| {
        sum.saturating_add(96)
            .saturating_add(key.len())
            .saturating_add(value_bytes(value))
    })
}

pub(super) fn bucket_bytes(bucket: &GroupBucket) -> usize {
    [
        &bucket.keys,
        &bucket.sums,
        &bucket.counts,

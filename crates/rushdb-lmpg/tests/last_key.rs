//! `derived::last_key` finds the largest key under a prefix with forward
//! seeks only; it must agree with a full scan on any key set, including keys
//! that are prefixes of others and bytes at 0x00 and 0xFF.

use std::collections::BTreeMap;

use rushdb_lmpg::derived::last_key;
use rushdb_lmpg::reader::TryRead;
use rushdb_storage::StorageError;

type KeyValueRows = Vec<(Vec<u8>, Vec<u8>)>;

struct Map(BTreeMap<Vec<u8>, Vec<u8>>);

impl TryRead for Map {
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.0.get(k).cloned())
    }
    fn range_prefix(&self, p: &[u8]) -> Result<KeyValueRows, StorageError> {
        Ok(self
            .0
            .iter()
            .filter(|(k, _)| k.starts_with(p))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
    fn range(&self, start: &[u8], end: &[u8]) -> Result<KeyValueRows, StorageError> {
        Ok(self
            .0
            .iter()
            .filter(|(k, _)| k.as_slice() >= start && (end.is_empty() || k.as_slice() < end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}


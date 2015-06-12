//! In-memory storage backend.
//!
//! The in-memory store is an ordinary [`StorageBackend`] whose MVCC maps are
//! the storage, so the durable and in-memory modes share one read path
//! ([`crate::backend::StorageReadView`]) and one write path
//! ([`StorageBackend::apply`]/`apply_many`). It is a test / small-data mode,
//! not a production scaling target.
//!
//! `Store` keeps shared `Arc` handles to the same maps for diagnostics and
//! replay; they are the *same* data, not a second copy.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::backend::{LoadedState, StorageBackend, StorageReadView};
use crate::read_view::{ReadLimits, ScanProjection};
use crate::{wal, Op, StorageError, StorageStatistics, VersionEntry, LAST_APPLIED_KEY, LOG_PREFIX};

/// Shared MVCC maps backing an in-memory store.
pub(crate) struct MemoryMaps {
    /// Current value per live key.
    pub(crate) data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    /// Creation commit index per live key (multi-version reads).
    pub(crate) created: Arc<RwLock<BTreeMap<Vec<u8>, u64>>>,
    /// Superseded values per key for time-travel reads.
    pub(crate) history: Arc<RwLock<BTreeMap<Vec<u8>, Vec<VersionEntry>>>>,
}

impl MemoryMaps {
    pub(crate) fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            created: Arc::new(RwLock::new(BTreeMap::new())),
            history: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

pub(crate) struct MemoryBackend {
    health: Arc<crate::health::StoreHealth>,
    maps: MemoryMaps,
    abort: Arc<AtomicBool>,
}

impl MemoryBackend {
    pub(crate) fn new(
        health: Arc<crate::health::StoreHealth>,
        maps: MemoryMaps,
        abort: Arc<AtomicBool>,
    ) -> Self {
        Self {
            health,
            maps,
            abort,
        }
    }

    /// Apply one op-set's MVCC transition to the maps. Mirrors the durable
    /// backend's semantics: `Put`/`Del` retain a superseded version, `Purge`
    /// erases history, `Set` overwrites without history.
    fn apply_ops(&self, ops: &[Op], ci: u64) -> Result<(), String> {
        let mut data = self
            .maps
            .data
            .write()
            .map_err(|_| "memory data lock poisoned".to_string())?;
        {
            let mut hist = self
                .maps
                .history
                .write()
                .map_err(|_| "memory history lock poisoned".to_string())?;
            let mut created = self
                .maps
                .created
                .write()
                .map_err(|_| "memory created lock poisoned".to_string())?;
            for op in ops {
                match op {
                    Op::Put(k, _) => {
                        if let Some(prev_created) = created.get(k).copied() {
                            let old = data.get(k).cloned();
                            hist.entry(k.clone()).or_default().push(VersionEntry {
                                from_ci: prev_created,
                                to_ci: ci,
                                value: old,
                            });
                        }
                        created.insert(k.clone(), ci);
                    }
                    Op::Del(k) => {
                        if let Some(prev_created) = created.get(k).copied() {
                            let old = data.get(k).cloned();
                            hist.entry(k.clone()).or_default().push(VersionEntry {
                                from_ci: prev_created,
                                to_ci: ci,
                                value: old,
                            });
                        }
                        created.remove(k);
                    }
                    Op::Purge(k) => {
                        hist.remove(k);
                        created.remove(k);
                    }
                    Op::Set(k, _) => {
                        hist.remove(k);
                        created.insert(k.clone(), ci);
                    }
                }
            }
        }
        let mut usage = crate::usage::UsageBytes::default();
        for op in ops {
            match op {
                Op::Put(k, v) | Op::Set(k, v) => {
                    let old = data.insert(k.clone(), v.clone());
                    usage.record(k, old.as_ref().map(Vec::len), Some(v.len()));
                }
                Op::Del(k) | Op::Purge(k) => {
                    let old = data.remove(k);
                    usage.record(k, old.as_ref().map(Vec::len), None);
                }
            }
        }
        let rows = usage.rows(|key| Ok(data.get(key).cloned()))?;
        if rows.is_empty() {
            return Ok(());
        }
        let usage_ops: Vec<Op> = rows
            .into_iter()
            .map(|(key, value)| match value {
                Some(value) => Op::Put(key, value),
                None => Op::Del(key),
            })
            .collect();
        drop(data);
        self.apply_ops(&usage_ops, ci)
    }
}

impl StorageBackend for MemoryBackend {
    fn load(&self) -> Result<LoadedState, String> {
        let data = self
            .maps
            .data
            .read()
            .map_err(|_| "memory data lock poisoned".to_string())?
            .clone();
        let created = self
            .maps
            .created
            .read()
            .map_err(|_| "memory created lock poisoned".to_string())?
            .clone();
        let history = self
            .maps
            .history
            .read()
            .map_err(|_| "memory history lock poisoned".to_string())?
            .clone();
        Ok(LoadedState {
            data,
            created,
            history,
            gc_watermark: 0,
        })
    }

    fn apply(&self, ops: &[Op], commit_index: u64) -> Result<(), String> {
        self.apply_ops(ops, commit_index)
    }

    fn apply_many(&self, batches: &[(u64, &[Op])]) -> Result<(), String> {
        for (ci, ops) in batches {
            self.apply_ops(ops, *ci)?;
        }
        Ok(())
    }

    fn retire_before(&self, watermark: u64) -> Result<(), String> {
        let mut hist = self
            .maps
            .history
            .write()
            .map_err(|_| "memory history lock poisoned".to_string())?;
        for entries in hist.values_mut() {

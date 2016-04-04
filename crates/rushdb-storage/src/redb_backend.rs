use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use crate::history_v2;
use crate::{
    Op, ReadLimits, ScanProjection, StorageError, StorageStatistics, VersionEntry, LAST_APPLIED_KEY,
};

const DATA: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rushdb.data.v1");
const HISTORY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rushdb.history.v1");
/// Sealed, append-only history blocks (P05 append-friendly segments). The
/// `HISTORY` row is the head (newest versions); older versions are sealed here
/// and linked by `prev`, so a hot-key append rewrites only the head.
const HISTORY_BLOCK: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("rushdb.history_block.v1");
/// Expiry-ordered secondary index over [`HISTORY`]: one row per retained
/// version, keyed `to_ci_be || data_key`, value empty. Lets GC range straight
/// to the expired versions instead of decoding every key's version list.
const HISTORY_EXPIRY: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("rushdb.history_expiry.v1");
/// Expiry rows carry no payload; the key alone identifies the version.
const EMPTY_VALUE: &[u8] = &[];
const META: TableDefinition<&str, u64> = TableDefinition::new("rushdb.meta.v1");
const CHECKPOINT_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("rushdb.checkpoint.v1");
const GC_WATERMARK: &str = "gc_watermark";
const CHECKPOINT_KEY: &[u8] = b"checkpoint";

/// Data rows carry the commit index that wrote the current version as an
/// 8-byte big-endian header in front of the value. This was a separate
/// `created` table keyed by every data key, which duplicated every key on disk
/// (~0.9 GB plus as much B-tree slack at 200K records), cost a second B-tree
/// write per op, and a second lookup per snapshot read (C10, cloud-capacity
/// F1/F4).
const VERSION_HEADER: usize = 8;

fn encode_current(commit_index: u64, value: &[u8]) -> Vec<u8> {
    let mut row = Vec::with_capacity(VERSION_HEADER + value.len());
    row.extend_from_slice(&commit_index.to_be_bytes());
    row.extend_from_slice(value);
    row
}

/// Split a data row into `(commit index that wrote it, value)`.
fn decode_current(row: &[u8]) -> Result<(u64, &[u8]), StorageError> {
    let header = row
        .get(..VERSION_HEADER)
        .and_then(|bytes| <[u8; VERSION_HEADER]>::try_from(bytes).ok())
        .ok_or(StorageError::CorruptLog)?;
    Ok((u64::from_be_bytes(header), &row[VERSION_HEADER..]))
}

/// The value part of a data row.
fn current_value(row: &[u8]) -> Result<&[u8], StorageError> {
    decode_current(row).map(|(_, value)| value)
}

/// All sealed-block keys for `data_key`, ascending.
fn block_keys(
    blocks: &redb::Table<'_, &[u8], &[u8]>,
    data_key: &[u8],
) -> Result<Vec<Vec<u8>>, String> {
    let prefix = history_v2::block_prefix(data_key);
    let (start, end) = crate::prefix_range(&prefix);
    let mut out = Vec::new();
    for row in blocks
        .range::<&[u8]>((
            Bound::Included(start.as_slice()),
            Bound::Excluded(end.as_slice()),
        ))
        .map_err(|e| e.to_string())?
    {
        out.push(row.map_err(|e| e.to_string())?.0.value().to_vec());
    }
    Ok(out)
}

/// Append one version to a key's history. The newest `<= BLOCK_SIZE` versions
/// live in the `HISTORY` head; when it overflows, the oldest are sealed into an
/// immutable `HISTORY_BLOCK` and linked by `prev`. A hot-key append therefore
/// rewrites only the bounded head, not the whole version list.
fn append_history(
    history: &mut redb::Table<'_, &[u8], &[u8]>,
    blocks: &mut redb::Table<'_, &[u8], &[u8]>,
    data_key: &[u8],
    version: VersionEntry,
) -> Result<(), String> {
    let existing = history
        .get(data_key)
        .map_err(|e| e.to_string())?
        .map(|value| value.value().to_vec());
    let (mut prev, mut versions) = match existing {
        Some(raw) => {
            history_v2::decode_block(&raw).map_err(|e| format!("{e}: corrupt history head"))?
        }
        None => (0u64, Vec::new()),
    };
    versions.insert(0, version); // newest first
    if versions.len() > history_v2::BLOCK_SIZE
        || history_v2::encode_block(prev, &versions).len() > history_v2::BLOCK_MAX_BYTES
    {
        // Seal a full BLOCK_SIZE of the oldest versions, leaving the newest in
        // the head. This happens once per BLOCK_SIZE appends, so a hot-key
        // append is amortized O(1) rather than rewriting the whole list.
        let mut take = versions.len().min(history_v2::BLOCK_SIZE);
        while take > 1
            && history_v2::encode_block(prev, &versions[versions.len() - take..]).len()
                > history_v2::BLOCK_MAX_BYTES
        {
            take -= 1;
        }
        let sealed = versions.split_off(versions.len() - take);
        let seq = sealed[0].to_ci;
        blocks
            .insert(
                history_v2::block_key(data_key, seq).as_slice(),
                history_v2::encode_block(prev, &sealed).as_slice(),
            )
            .map_err(|e| e.to_string())?;
        prev = seq;
    }
    history
        .insert(
            data_key,
            history_v2::encode_block(prev, &versions).as_slice(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Replace a key's history with `kept` (newest first), deleting every sealed
/// block and rewriting the head/chain.
fn write_chain(
    history: &mut redb::Table<'_, &[u8], &[u8]>,
    blocks: &mut redb::Table<'_, &[u8], &[u8]>,
    data_key: &[u8],
    kept: Vec<VersionEntry>,
) -> Result<(), String> {
    for block in block_keys(blocks, data_key)? {
        blocks.remove(block.as_slice()).map_err(|e| e.to_string())?;
    }
    if kept.is_empty() {
        history.remove(data_key).map_err(|e| e.to_string())?;
        return Ok(());
    }
    let mut remaining = kept;
    let mut chunks: Vec<Vec<VersionEntry>> = Vec::new();
    while !remaining.is_empty() {
        let mut take = remaining.len().min(history_v2::BLOCK_SIZE);
        while take > 1
            && history_v2::encode_block(0, &remaining[..take]).len() > history_v2::BLOCK_MAX_BYTES
        {
            take -= 1;
        }
        chunks.push(remaining.drain(..take).collect());
    }
    let head = chunks.remove(0);
    let mut prev = 0u64;
    for chunk in chunks.into_iter().rev() {
        let seq = chunk[0].to_ci;
        blocks
            .insert(
                history_v2::block_key(data_key, seq).as_slice(),
                history_v2::encode_block(prev, &chunk).as_slice(),
            )
            .map_err(|e| e.to_string())?;
        prev = seq;
    }
    history
        .insert(data_key, history_v2::encode_block(prev, &head).as_slice())
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Drop a key's retained history (head + sealed blocks) and every expiry-index
/// row that points at it. Used by `Set`/`Purge`, which overwrite or erase
/// without retaining a version.
fn drop_history_indexed(
    history: &mut redb::Table<'_, &[u8], &[u8]>,
    blocks: &mut redb::Table<'_, &[u8], &[u8]>,
    expiry: &mut redb::Table<'_, &[u8], &[u8]>,
    key: &[u8],
) -> Result<(), String> {
    let mut drop_expiry = |versions: &[VersionEntry]| -> Result<(), String> {
        for version in versions {
            if version.to_ci != 0 {
                expiry
                    .remove(history_v2::expiry_key(version.to_ci, key).as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    };
    if let Some(raw) = history.get(key).map_err(|e| e.to_string())? {
        let (_prev, versions) =
            history_v2::decode_block(raw.value()).map_err(|e| format!("{e}: corrupt history"))?;
        drop_expiry(&versions)?;
    }
    history.remove(key).map_err(|e| e.to_string())?;
    for block in block_keys(blocks, key)? {
        if let Some(raw) = blocks.get(block.as_slice()).map_err(|e| e.to_string())? {
            let (_prev, versions) = history_v2::decode_block(raw.value())
                .map_err(|e| format!("{e}: corrupt history block"))?;
            drop_expiry(&versions)?;
        }
        blocks.remove(block.as_slice()).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Reclaim versions with `to_ci <= watermark` for one key, removing their
/// expiry rows and rebuilding the chain from the survivors.
fn prune_history(
    history: &mut redb::Table<'_, &[u8], &[u8]>,
    blocks: &mut redb::Table<'_, &[u8], &[u8]>,
    expiry: &mut redb::Table<'_, &[u8], &[u8]>,
    data_key: &[u8],
    watermark: u64,
) -> Result<(), String> {
    let mut kept: Vec<VersionEntry> = Vec::new();
    let mut next: Option<u64> = None;
    loop {
        let raw = match next {
            None => history
                .get(data_key)
                .map_err(|e| e.to_string())?
                .map(|value| value.value().to_vec()),
            Some(seq) => blocks
                .get(history_v2::block_key(data_key, seq).as_slice())
                .map_err(|e| e.to_string())?
                .map(|value| value.value().to_vec()),
        };
        let Some(raw) = raw else {
            break;
        };
        let (prev, versions) =
            history_v2::decode_block(&raw).map_err(|e| format!("{e}: corrupt history block"))?;
        for version in versions {
            if version.to_ci == 0 || version.to_ci > watermark {
                kept.push(version);
            } else {
                expiry
                    .remove(history_v2::expiry_key(version.to_ci, data_key).as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
        if prev == 0 {
            break;
        }
        next = Some(prev);
    }
    write_chain(history, blocks, data_key, kept)
}

/// Shared per-op MVCC application against an open redb write transaction.
fn apply_in_txn(tx: &redb::WriteTransaction, ops: &[Op], commit_index: u64) -> Result<(), String> {
    let mut data = tx.open_table(DATA).map_err(|e| e.to_string())?;
    let mut history = tx.open_table(HISTORY).map_err(|e| e.to_string())?;
    let mut blocks = tx.open_table(HISTORY_BLOCK).map_err(|e| e.to_string())?;
    let mut expiry = tx.open_table(HISTORY_EXPIRY).map_err(|e| e.to_string())?;
    let mut tables = MvccTables {
        data: &mut data,
        history: &mut history,
        blocks: &mut blocks,
        expiry: &mut expiry,
    };
    let mut usage = crate::usage::UsageBytes::default();
    for op in ops {
        apply_op(&mut tables, op, commit_index, &mut usage)?;
    }
    let rows = usage.rows(|key| {
        tables
            .data
            .get(key)
            .map_err(|e| e.to_string())?
            .map(|row| {
                current_value(row.value())
                    .map(<[u8]>::to_vec)
                    .map_err(|e| e.to_string())
            })
            .transpose()
    })?;
    for (key, value) in rows {
        let op = match value {
            Some(value) => Op::Put(key, value),
            None => Op::Del(key),
        };
        apply_op(
            &mut tables,
            &op,
            commit_index,
            &mut crate::usage::UsageBytes::default(),
        )?;
    }
    Ok(())
}

/// The open MVCC tables of one redb write transaction.
struct MvccTables<'a, 't> {
    data: &'a mut redb::Table<'t, &'static [u8], &'static [u8]>,
    history: &'a mut redb::Table<'t, &'static [u8], &'static [u8]>,
    blocks: &'a mut redb::Table<'t, &'static [u8], &'static [u8]>,
    expiry: &'a mut redb::Table<'t, &'static [u8], &'static [u8]>,
}

/// Apply one op's MVCC transition, recording its byte change in `usage`.
fn apply_op(
    tables: &mut MvccTables<'_, '_>,
    op: &Op,
    commit_index: u64,
    usage: &mut crate::usage::UsageBytes,
) -> Result<(), String> {
    let MvccTables {
        data,
        history,
        blocks,
        expiry,
    } = tables;
    {
        let key = match op {
            Op::Put(key, _) | Op::Del(key) | Op::Set(key, _) | Op::Purge(key) => key,
        };
        // Purge is destructive: erase the key's current value, metadata and
        // retained history without appending a superseded version.
        if matches!(op, Op::Purge(_)) {
            let old = data.remove(key.as_slice()).map_err(|e| e.to_string())?;
            let old = old
                .map(|row| current_value(row.value()).map(<[u8]>::len))
                .transpose()
                .map_err(|e| e.to_string())?;
            usage.record(key, old, None);
            drop_history_indexed(history, blocks, expiry, key)?;
            return Ok(());
        }
        // Set overwrites internal bookkeeping without retaining a version:
        // insert the new value, advance the creation index, drop any history
        // already accumulated for the key (bounds hot-counter write cost).
        if let Op::Set(_, value) = op {
            let old = data
                .insert(
                    key.as_slice(),
                    encode_current(commit_index, value).as_slice(),
                )
                .map_err(|e| e.to_string())?;
            let old = old
                .map(|row| current_value(row.value()).map(<[u8]>::len))
                .transpose()
                .map_err(|e| e.to_string())?;
            usage.record(key, old, Some(value.len()));
            drop_history_indexed(history, blocks, expiry, key)?;
            return Ok(());
        }
        let previous = data
            .get(key.as_slice())
            .map_err(|e| e.to_string())?
            .map(|row| {
                decode_current(row.value())
                    .map(|(from_ci, value)| (from_ci, value.to_vec()))
                    .map_err(|e| e.to_string())
            })
            .transpose()?;

        usage.record(
            key,
            previous.as_ref().map(|(_, value)| value.len()),
            match op {
                Op::Put(_, value) => Some(value.len()),
                _ => None,
            },
        );
        if let Some((from_ci, previous_value)) = previous {
            append_history(
                history,
                blocks,
                key,
                VersionEntry {
                    from_ci,
                    to_ci: commit_index,
                    value: Some(previous_value),
                },
            )?;
            expiry
                .insert(
                    history_v2::expiry_key(commit_index, key).as_slice(),
                    EMPTY_VALUE,
                )
                .map_err(|e| e.to_string())?;
        }

        match op {
            Op::Put(key, value) => {
                data.insert(
                    key.as_slice(),
                    encode_current(commit_index, value).as_slice(),
                )
                .map_err(|e| e.to_string())?;
            }
            Op::Del(key) => {
                data.remove(key.as_slice()).map_err(|e| e.to_string())?;
            }
            Op::Purge(_) => unreachable!("purge is handled before version bookkeeping"),
            Op::Set(..) => unreachable!("set is handled before version bookkeeping"),
        }
    }
    Ok(())
}

pub(crate) struct RedbState {
    pub(crate) data: BTreeMap<Vec<u8>, Vec<u8>>,
    pub(crate) created: BTreeMap<Vec<u8>, u64>,
    pub(crate) history: BTreeMap<Vec<u8>, Vec<VersionEntry>>,
    pub(crate) gc_watermark: u64,
}

/// Crash-safe durable representation of the `RushDB` ordered keyspace.
///
/// redb owns page allocation, checksums, recovery, MVCC and fsync semantics.
/// `RushDB` only maps its logical key/value operations and historical metadata
/// onto redb tables in one ACID write transaction.
pub(crate) struct RedbBackend {
    /// redb is MVCC: read transactions never wait for the writer, and it
    /// serializes write transactions internally. Every read and write path
    /// therefore takes the **shared** guard; only physical compaction (which
    /// needs `&mut Database`) takes the exclusive one. A plain mutex here made
    /// every `read_view()` wait for the whole write transaction, page-miss I/O
    /// included (C1, cloud-capacity F3).
    db: RwLock<Database>,
    // Declared after db so the cache is destroyed before its charge releases.
    _cache_reservation: crate::resources::ByteReservation,
    shard_id: u32,
    /// Set at shutdown so long maintenance scans (e.g. byte accounting) stop
    /// within a batch instead of delaying a clean close.
    abort: Arc<AtomicBool>,
    /// Cumulative nanoseconds spent waiting to acquire the redb database lock
    /// (RM-7 backend-lock measurement).
    mutex_wait_ns: AtomicU64,
    /// Cumulative nanoseconds the redb database lock was held (any mode).
    mutex_hold_ns: AtomicU64,
    /// Maximum single hold time of the redb database lock, in nanoseconds.
    mutex_hold_max_ns: AtomicU64,
    /// Write-transaction holds by holder.
    writer_holds: WriterHoldCounters,
    /// RM-8 fault injection: fail the next `apply`/`apply_many`.
    #[cfg(test)]
    fail_next_apply: AtomicBool,
    /// RM-8 fault injection: fail the next `retire_before`.
    #[cfg(test)]
    fail_next_retire: AtomicBool,
    /// RM-8 fault injection: fail the next `compact`.
    #[cfg(test)]
    fail_next_compact: AtomicBool,
    live_reads: Arc<LiveReads>,
}

/// Longest work one history-GC batch does while holding the writer. Keys
/// with long version chains made a 512-key batch hold it for over a second
/// beside a bulk load, stalling every tenant's commits (SCALE-2, 2026-10-19:
/// 2 of 4,197 batches over 1 s, 14 over 0.25 s).
const HISTORY_GC_BATCH_MAX_HOLD: std::time::Duration = std::time::Duration::from_millis(100);

/// Per-holder writer-hold histograms: counts per bucket of
/// [`crate::WRITER_HOLD_BOUNDS`] (last slot: above every bound) and the
/// total nanoseconds held.
#[derive(Default)]
struct WriterHoldCounters {
    counts: [[AtomicU64; crate::WRITER_HOLD_BOUNDS.len() + 1]; crate::WriterHolder::ALL.len()],
    sum_ns: [AtomicU64; crate::WriterHolder::ALL.len()],
}

impl WriterHoldCounters {
    fn record(&self, holder: crate::WriterHolder, held: std::time::Duration) {
        let seconds = held.as_secs_f64();
        let bucket = crate::WRITER_HOLD_BOUNDS
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(crate::WRITER_HOLD_BOUNDS.len());
        self.counts[holder.index()][bucket].fetch_add(1, Ordering::Relaxed);
        self.sum_ns[holder.index()].fetch_add(
            u64::try_from(held.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> Vec<(crate::WriterHolder, crate::WriterHolds)> {
        crate::WriterHolder::ALL
            .into_iter()
            .map(|holder| {
                let mut holds = crate::WriterHolds::default();
                for (count, counter) in holds.counts.iter_mut().zip(&self.counts[holder.index()]) {
                    *count = counter.load(Ordering::Relaxed);
                }
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "a metric sum; nanosecond precision past 2^53 is not needed"
                )]
                let seconds = self.sum_ns[holder.index()].load(Ordering::Relaxed) as f64 / 1e9;
                holds.sum_seconds = seconds;
                (holder, holds)
            })
            .collect()
    }
}

/// Times one write transaction from before `begin_write` until it drops,
/// after the commit or abort.
struct WriterHold<'a> {
    counters: &'a WriterHoldCounters,
    holder: crate::WriterHolder,
    started: std::time::Instant,
}

impl Drop for WriterHold<'_> {
    fn drop(&mut self) {
        self.counters.record(self.holder, self.started.elapsed());
    }
}

/// A redb database-lock guard (shared or exclusive) that records wait/hold
/// durations on drop.
struct TimedDbGuard<'a, G> {
    inner: G,
    held_at: std::time::Instant,
    hold_ns: &'a AtomicU64,
    hold_max_ns: &'a AtomicU64,
}

impl<G: std::ops::Deref<Target = Database>> std::ops::Deref for TimedDbGuard<'_, G> {
    type Target = Database;
    fn deref(&self) -> &Database {
        &self.inner

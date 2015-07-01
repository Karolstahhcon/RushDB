//! Segmented, append-only write-ahead log (log-first).
//!
//! The log-first substrate makes this log the **durable truth**: a commit is
//! acknowledged only after its framed record is appended and fsynced, and the
//! redb materialization is recovered by replaying log records beyond its
//! recorded checkpoint LSN. See the manual's
//! `docs/manual/architecture/storage-engine.md`.
//!
//! Layout: one directory of segment files named by their first LSN
//! (`00000000000000000001.seg`), each with a fixed header followed by
//! self-describing records produced by [`crate::wal::encode_wal_record`]. A
//! record's own 32-byte header carries its payload length and CRC, so a torn
//! trailing write (a crash mid-append) is detectable and truncatable, while a
//! torn or corrupt record in a sealed segment is a hard error.
//!
//! This module is additive: it is not yet wired into the publisher. Phase 1 of
//! the plan wires it as the durable acknowledgement and demotes redb to a
//! checkpointed materialization.

use crate::wal::{decode_wal_record, WAL_FORMAT_VERSION};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Magic identifying a live log segment file.
const LOG_SEGMENT_MAGIC: [u8; 8] = *b"RDBWSEG1";

/// Segment header: magic(8) + format version(4) + shard id(4) + first LSN(8).
const LOG_SEGMENT_HEADER_LEN: usize = 8 + 4 + 4 + 8;

/// A framed record's fixed header, as emitted by
/// [`crate::wal::encode_wal_record`]: magic(8) + version(4) + shard(4) +
/// lsn(8) + payload length(4) + crc(4).
const LOG_RECORD_HEADER_LEN: usize = 32;

/// Record ceiling, matching [`crate::wal`]. `pub(crate)` so `Store::append_log`
/// can size a chained commit's fragments against the same hard ceiling this
/// module enforces, rather than duplicating the literal.
pub(crate) const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// Errors returned by the segmented log.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// An underlying file operation failed.
    #[error("log I/O error: {0}")]
    Io(#[from] io::Error),
    /// A segment failed structural validation.
    #[error("corrupt log segment {path}: {detail}")]
    Corrupt {
        /// Path of the offending segment.
        path: String,
        /// Why the segment is rejected.
        detail: String,
    },
    /// A record was appended with an LSN that breaks the contiguous sequence.
    #[error("log append out of order: expected LSN {expected}, got {actual}")]
    OutOfOrder {
        /// The LSN the log required next.
        expected: u64,
        /// The LSN the caller supplied.
        actual: u64,
    },
    /// A record exceeds the 64 MiB ceiling.
    #[error("log record {0} bytes exceeds the 64 MiB ceiling")]
    Oversized(usize),
}

/// One sealed or active log segment's in-memory descriptor.
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    /// First LSN stored in the segment.
    pub first_lsn: u64,
    /// Last LSN stored in the segment (`first_lsn - 1` when empty).
    pub last_lsn: u64,
    /// Segment file path.
    pub path: PathBuf,
    /// Valid bytes in the segment (after any torn tail is truncated).
    pub bytes: u64,
}

/// The currently-open segment receiving appends.
#[derive(Debug)]
struct ActiveSegment {
    file: File,
    first_lsn: u64,
    bytes: u64,
}

/// A directory of append-only log segments with a contiguous LSN sequence.
#[derive(Debug)]
pub struct LogStore {
    dir: PathBuf,
    shard_id: u32,
    segment_bytes: u64,
    segments: Vec<SegmentInfo>,
    next_lsn: u64,
    active: Option<ActiveSegment>,
}

impl LogStore {
    /// Open (creating if absent) a log directory and recover its state.
    ///
    /// Existing segments are validated in order. A torn trailing record in the
    /// **last** segment is truncated away (an interrupted append); the same
    /// damage in a sealed segment is [`LogError::Corrupt`]. The next expected
    /// LSN is one past the highest recovered record (1 for an empty log).
    ///
    /// # Errors
    /// Returns [`LogError`] if the directory cannot be created or read, or a
    /// segment is corrupt.
    pub fn open(dir: &Path, shard_id: u32, segment_bytes: u64) -> Result<Self, LogError> {
        std::fs::create_dir_all(dir)?;
        let mut paths: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().into_string().ok()?;
                let first_lsn = name.strip_suffix(".seg")?.parse::<u64>().ok()?;
                Some((first_lsn, entry.path()))
            })
            .collect();
        paths.sort_by_key(|(first_lsn, _)| *first_lsn);

        let mut segments = Vec::with_capacity(paths.len());
        let mut next_lsn = 1u64;
        let last_index = paths.len().saturating_sub(1);
        for (index, (first_lsn, path)) in paths.iter().enumerate() {
            let is_active = index == last_index;
            let (records, valid_bytes, file_len) = read_segment(path, shard_id, is_active)?;
            if valid_bytes != file_len {
                if !is_active {
                    return Err(LogError::Corrupt {
                        path: path.display().to_string(),
                        detail: "sealed segment ends with a torn record".into(),
                    });
                }
                // Interrupted append in the active segment: drop the tail so
                // later appends resume on a clean record boundary.
                let file = OpenOptions::new().write(true).open(path)?;
                file.set_len(valid_bytes)?;
                file.sync_all()?;
            }
            let last_lsn = records
                .last()
                .map_or(first_lsn.saturating_sub(1), |(lsn, _)| *lsn);
            next_lsn = last_lsn.saturating_add(1);
            segments.push(SegmentInfo {
                first_lsn: *first_lsn,
                last_lsn,
                path: path.clone(),
                bytes: valid_bytes,
            });
        }

        // Reuse the last segment for appends when it still has room; otherwise
        // the next append starts a fresh segment.
        let active = match segments.last() {
            Some(last) if last.bytes < segment_bytes => {
                let file = OpenOptions::new().append(true).open(&last.path)?;
                Some(ActiveSegment {
                    file,
                    first_lsn: last.first_lsn,
                    bytes: last.bytes,
                })
            }
            _ => None,
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            shard_id,
            segment_bytes,
            segments,
            next_lsn,
            active,
        })
    }

    /// Seed `next_lsn` from `floor` when this log has no existing segments
    /// (a freshly created, never-before-written directory) and `floor`
    /// exceeds the default start of 1. For opening the WAL beside a
    /// materialization whose checkpoint is already ahead of an empty WAL —
    /// a store opened from a restored base backup with no accompanying WAL
    /// directory, for example — so the next commit continues the sequence
    /// instead of colliding with LSNs the materialization already has. A
    /// no-op once any segment exists: a real recovered `next_lsn` is
    /// authoritative and must never be overridden.
    pub(crate) fn seed_next_lsn_if_empty(&mut self, floor: u64) {
        if self.segments.is_empty() && self.active.is_none() && floor > self.next_lsn {
            self.next_lsn = floor;
        }
    }

    /// The physical shard this log belongs to.
    #[must_use]
    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    /// The LSN the next appended record must carry.
    #[must_use]
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// The highest LSN appended so far (`0` when empty). After [`Self::sync`],
    /// this is the durable LSN.
    #[must_use]
    pub fn durable_lsn(&self) -> u64 {
        self.segments.last().map_or(0, |segment| segment.last_lsn)
    }

    /// The segment descriptors in LSN order.
    #[must_use]
    pub fn segments(&self) -> &[SegmentInfo] {
        &self.segments
    }

    /// Append framed records (each produced by [`crate::wal::encode_wal_record`]
    /// or [`crate::wal::encode_chained_ops_record`]) with contiguous, ascending
    /// LSNs. **Chaining (large-transaction support):** several
    /// consecutive entries may share one LSN — a chained commit's fragments,
    /// which must all be present in this same call (never split across two
    /// `append_batch` calls: see the caller's own invariant in
    /// `Store::append_log`) — and are grouped and written as one atomic unit:
    /// `next_lsn` advances only once per group, and the whole group is kept
    /// in one segment file (rotating *before* its first fragment if it would
    /// not otherwise fit, rather than splitting it across two segments).
    ///
    /// Records are written to the active segment, rotating to a new segment
    /// once the configured size is reached. The write is buffered by the OS;
    /// call [`Self::sync`] to make it durable before acknowledging.
    ///
    /// # Errors
    /// Returns [`LogError::OutOfOrder`] if an LSN is not the expected next LSN,
    /// [`LogError::Oversized`] if a record exceeds the ceiling, or
    /// [`LogError::Io`] on write failure.
    pub fn append_batch(&mut self, records: &[(u64, Vec<u8>)]) -> Result<(), LogError> {
        let mut i = 0;
        while i < records.len() {
            let lsn = records[i].0;
            if lsn != self.next_lsn {
                return Err(LogError::OutOfOrder {
                    expected: self.next_lsn,
                    actual: lsn,
                });
            }
            let mut group_end = i + 1;
            while group_end < records.len() && records[group_end].0 == lsn {
                group_end += 1;
            }
            let group = &records[i..group_end];
            for (_, framed) in group {
                if framed.len() > MAX_RECORD_BYTES {
                    return Err(LogError::Oversized(framed.len()));
                }
            }
            let group_bytes: u64 = group.iter().map(|(_, framed)| framed.len() as u64).sum();
            let would_overflow = self.active.as_ref().is_some_and(|active| {
                active.bytes > 0 && active.bytes + group_bytes > self.segment_bytes
            });
            if would_overflow {
                self.seal_active()?;
            }
            if self.active.is_none() {
                self.create_active(lsn)?;
            }
            for (_, framed) in group {
                let active = self.active.as_mut().ok_or_else(|| LogError::Corrupt {
                    path: self.dir.display().to_string(),
                    detail: "active segment missing after creation".into(),
                })?;
                active.file.write_all(framed)?;
                active.bytes += framed.len() as u64;
                if let Some(last) = self
                    .segments
                    .last_mut()
                    .filter(|last| last.first_lsn == active.first_lsn)
                {
                    last.last_lsn = lsn;
                    last.bytes = active.bytes;
                }
            }
            self.next_lsn = lsn.saturating_add(1);
            i = group_end;
        }
        Ok(())
    }

    /// Make all appended records durable (fsync the active segment and the
    /// directory so a freshly created segment name survives a crash).
    ///
    /// # Errors
    /// Returns [`LogError::Io`] if the fsync fails.
    pub fn sync(&mut self) -> Result<(), LogError> {
        if let Some(active) = self.active.as_mut() {
            active.file.sync_data()?;
        }
        sync_dir(&self.dir)?;
        Ok(())
    }

    /// Read every record with LSN `>= from_lsn`, in order, as framed bytes.
    ///
    /// # Errors
    /// Returns [`LogError`] if a segment cannot be read or is corrupt.
    pub fn read_from(&self, from_lsn: u64) -> Result<Vec<(u64, Vec<u8>)>, LogError> {
        let last_index = self.segments.len().saturating_sub(1);
        let mut out = Vec::new();
        for (index, segment) in self.segments.iter().enumerate() {
            if segment.last_lsn < from_lsn {
                continue;
            }
            let (records, _, _) = read_segment(&segment.path, self.shard_id, index == last_index)?;
            for (lsn, framed) in records {
                if lsn >= from_lsn {
                    out.push((lsn, framed));
                }
            }
        }
        Ok(out)
    }

    /// Descriptors of the segments that may hold LSNs in `[from_lsn,
    /// until_lsn)`, each flagged when it is the active (last) segment. Callers
    /// read them with [`read_segment_records`] **after** releasing the log
    /// lock, so a retention sweep never blocks WAL appends while it reads.
    #[must_use]
    pub fn segments_in(&self, from_lsn: u64, until_lsn: u64) -> Vec<(SegmentInfo, bool)> {
        let last_index = self.segments.len().saturating_sub(1);
        self.segments
            .iter()
            .enumerate()
            .filter(|(_, segment)| segment.last_lsn >= from_lsn && segment.first_lsn < until_lsn)
            .map(|(index, segment)| (segment.clone(), index == last_index))
            .collect()
    }

    /// Delete sealed segments that end before `lsn`. The active segment is
    /// never removed, so the log always retains its live tail. Returns the
    /// number of segments deleted.
    ///
    /// # Errors
    /// Returns [`LogError::Io`] if a segment file cannot be removed.
    pub fn truncate_before(&mut self, lsn: u64) -> Result<usize, LogError> {
        let active_first = self.active.as_ref().map(|active| active.first_lsn);
        let mut removed = 0;
        let mut keep = Vec::with_capacity(self.segments.len());
        for segment in &self.segments {
            let is_active = active_first == Some(segment.first_lsn);
            if !is_active && segment.last_lsn < lsn {
                std::fs::remove_file(&segment.path)?;
                removed += 1;
            } else {
                keep.push(segment.clone());
            }
        }
        self.segments = keep;
        Ok(removed)
    }

    /// Seal the active segment: fsync it, then stop appending to it.
    fn seal_active(&mut self) -> Result<(), LogError> {
        if let Some(active) = self.active.take() {
            active.file.sync_all()?;
        }
        sync_dir(&self.dir)?;
        Ok(())
    }

    /// Create and open a new segment whose first record is at `first_lsn`.
    fn create_active(&mut self, first_lsn: u64) -> Result<(), LogError> {
        let path = self.dir.join(segment_file_name(first_lsn));
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let mut header = Vec::with_capacity(LOG_SEGMENT_HEADER_LEN);
        header.extend_from_slice(&LOG_SEGMENT_MAGIC);
        header.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        header.extend_from_slice(&self.shard_id.to_le_bytes());
        header.extend_from_slice(&first_lsn.to_le_bytes());
        file.write_all(&header)?;
        self.segments.push(SegmentInfo {
            first_lsn,
            last_lsn: first_lsn.saturating_sub(1),
            path,
            bytes: LOG_SEGMENT_HEADER_LEN as u64,
        });
        self.active = Some(ActiveSegment {
            file,
            first_lsn,
            bytes: LOG_SEGMENT_HEADER_LEN as u64,
        });
        sync_dir(&self.dir)?;
        Ok(())
    }
}

/// Segment file name: zero-padded first LSN so lexical order is LSN order.
fn segment_file_name(first_lsn: u64) -> String {
    format!("{first_lsn:020}.seg")
}

/// fsync a directory so file creations/removals survive a crash.
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Outcome of trying to fill a buffer from a reader.
enum ReadFull {
    /// The buffer was filled.
    Full,
    /// The reader was already at end of stream.
    CleanEof,
    /// The stream ended partway through the buffer.
    Partial,
}

fn read_full<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<ReadFull> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => {
                return Ok(if filled == 0 {
                    ReadFull::CleanEof
                } else {
                    ReadFull::Partial
                });
            }
            Ok(n) => filled += n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(ReadFull::Full)
}

/// Read one segment: validate its header and records, returning the decoded
/// `(lsn, framed_bytes)` records and the byte offset after the last valid
/// record. When `allow_tail` is set, a torn trailing record is tolerated
/// (recovery of the active segment); otherwise it is corruption. The same
/// tolerance (truncate in the active segment, hard error in a sealed one)
/// applies to a **logically** incomplete trailing chain (a large
/// transaction) — a run of physically well-formed
/// `CHAIN_CONTINUE` records with no terminating `CHAIN_FINAL`, from a crash
/// mid-chain — even though each individual record in it passes its own
/// checksum. A sealed segment can never legitimately end mid-chain: writing
/// one never spans two `append_batch` calls, and appends and retention
/// truncation share one lock, so a chain is always either fully durable or
/// never started by the time any other segment could have been sealed.
/// Read one segment's framed records (see [`LogStore::segments_in`]).
///
/// # Errors
/// Returns [`LogError`] if the segment cannot be read or is corrupt.
pub fn read_segment_records(
    path: &Path,
    shard_id: u32,
    allow_tail: bool,
) -> Result<Vec<(u64, Vec<u8>)>, LogError> {
    read_segment(path, shard_id, allow_tail).map(|(records, _, _)| records)
}

fn read_segment(
    path: &Path,
    shard_id: u32,
    allow_tail: bool,
) -> Result<(Vec<(u64, Vec<u8>)>, u64, u64), LogError> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let corrupt = |detail: String| LogError::Corrupt {
        path: path.display().to_string(),
        detail,
    };

    let mut header = [0u8; LOG_SEGMENT_HEADER_LEN];
    if !matches!(read_full(&mut file, &mut header)?, ReadFull::Full) {

//! Record framing for the log-first storage substrate. This **is** the
//! write-ahead log: [`crate::log_store`] appends
//! these framed records to segment files, and that durable append is the
//! commit's ack boundary (not the buffered redb materialization). See the
//! manual's `docs/manual/architecture/storage-engine.md`.
//!
//! Every record (an expanded op-set, optionally paired with its originating
//! [`CommitBatch`] — [`encode_ops_record`]) is wrapped in a fixed
//! header with magic, format version, shard id, LSN, payload length, and a
//! CRC32C-style checksum. This lets replay/export/CDC distinguish valid old
//! data, valid new data, partial new data, and corrupted data instead of
//! hoping the filesystem write was atomic.
//!
//! Greenfield: only the framed format is read or written. There is no legacy
//! unframed format to migrate. An in-memory store (no WAL file) instead keeps
//! a row-based command journal — the one remaining user of the bare
//! `__log__/<lsn>` row format.

use crate::Op;
use crc32fast::Hasher as Crc32;
use rushdb_types::command::CommitBatch;
use std::io;

/// Magic bytes identifying a `RushDB` logical WAL record.
const WAL_MAGIC: [u8; 8] = *b"RDBWAL01";

/// Current logical WAL framing format version.
pub const WAL_FORMAT_VERSION: u32 = 2;

/// Header size: magic(8) + version(4) + `shard_id(4)` + lsn(8) + length(4) + crc(4).
const WAL_HEADER_LEN: usize = 8 + 4 + 4 + 8 + 4 + 4;

/// A decoded logical WAL record header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeader {
    /// Physical shard that produced the record.
    pub shard_id: u32,
    /// Log sequence number (commit index) of the record.
    pub lsn: u64,
    /// Length of the payload that follows the header.
    pub payload_len: u32,
}

/// Error returned when a logical WAL record fails to decode or verify.
#[derive(Debug, thiserror::Error)]
pub enum WalDecodeError {
    /// The record is shorter than its header or declared payload.
    #[error("short WAL record: expected {expected} bytes, got {actual}")]
    Truncated {
        /// Total length required by the header/payload.
        expected: usize,
        /// Actual bytes available.
        actual: usize,
    },
    /// The record does not begin with the expected magic.
    #[error("invalid WAL magic")]
    BadMagic,
    /// The record uses a framing version this build does not support.
    #[error("unsupported WAL format version {0}")]
    UnsupportedVersion(u32),
    /// The record's CRC does not match its contents.
    #[error("WAL record checksum mismatch")]
    ChecksumMismatch,
    /// The declared payload exceeds the 64 MiB record ceiling.
    #[error("payload length {0} exceeds the 64 MiB record ceiling")]
    OversizedPayload(u32),
}

/// Encode one logical record into a framed, checksummed byte vector.
///
/// # Panics
/// Panics if `payload` is larger than `u32::MAX` bytes, which the 64 MiB
/// record ceiling makes impossible in practice.
#[must_use]
pub fn encode_wal_record(shard_id: u32, lsn: u64, payload: &[u8]) -> Vec<u8> {
    let payload_len = u32::try_from(payload.len()).expect("payload length fits u32");
    let mut out = Vec::with_capacity(WAL_HEADER_LEN + payload.len());
    out.extend_from_slice(&WAL_MAGIC);
    out.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&shard_id.to_le_bytes());
    out.extend_from_slice(&lsn.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    // Checksum covers everything after the length field (length field itself is
    // integrity-checked by the outer redb row checksum; include header+payload
    // for a RustDB-level end-to-end guard).
    let mut hasher = Crc32::new();
    hasher.update(&WAL_MAGIC);
    hasher.update(&WAL_FORMAT_VERSION.to_le_bytes());
    hasher.update(&shard_id.to_le_bytes());
    hasher.update(&lsn.to_le_bytes());
    hasher.update(&payload_len.to_le_bytes());
    hasher.update(payload);
    out.extend_from_slice(&hasher.finalize().to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode and verify a framed record; returns the header and payload.
///
/// # Errors
/// Returns [`WalDecodeError`] when the record is truncated, has bad magic, an
/// unsupported version, an oversized payload, or a checksum mismatch.
///
/// # Panics
/// Panics only if an internal fixed-width slice conversion fails after its
/// length has been bounds-checked, which cannot happen.
pub fn decode_wal_record(raw: &[u8]) -> Result<(WalHeader, &[u8]), WalDecodeError> {
    if raw.len() < WAL_HEADER_LEN {
        return Err(WalDecodeError::Truncated {
            expected: WAL_HEADER_LEN,
            actual: raw.len(),
        });
    }
    if raw[0..8] != WAL_MAGIC {
        return Err(WalDecodeError::BadMagic);
    }
    let version = u32::from_le_bytes(raw[8..12].try_into().expect("slice"));
    if version != WAL_FORMAT_VERSION {
        return Err(WalDecodeError::UnsupportedVersion(version));
    }
    let shard_id = u32::from_le_bytes(raw[12..16].try_into().expect("slice"));
    let lsn = u64::from_le_bytes(raw[16..24].try_into().expect("slice"));
    let payload_len = u32::from_le_bytes(raw[24..28].try_into().expect("slice"));
    if payload_len > 64 * 1024 * 1024 {
        return Err(WalDecodeError::OversizedPayload(payload_len));
    }
    let expected = WAL_HEADER_LEN + payload_len as usize;
    if raw.len() < expected {
        return Err(WalDecodeError::Truncated {
            expected,
            actual: raw.len(),
        });
    }
    let stored_crc = u32::from_le_bytes(raw[28..32].try_into().expect("slice"));
    let mut hasher = Crc32::new();
    hasher.update(&raw[0..28]);
    hasher.update(&raw[WAL_HEADER_LEN..expected]);
    let computed = hasher.finalize();
    if stored_crc != computed {
        return Err(WalDecodeError::ChecksumMismatch);
    }
    Ok((
        WalHeader {
            shard_id,
            lsn,
            payload_len,
        },
        &raw[WAL_HEADER_LEN..expected],
    ))
}

/// Wall-clock now in milliseconds since the Unix epoch (used for retention
/// lease deadlines and checkpoint publication timestamps).
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Encode the `LAST_APPLIED` watermark row (a framed record with an empty
/// payload carries the LSN in the header).
#[must_use]
pub fn encode_last_applied(shard_id: u32, lsn: u64) -> Vec<u8> {
    encode_wal_record(shard_id, lsn, &[])
}

/// Decode the `LAST_APPLIED` watermark row.
///
/// # Errors
/// Returns a human-readable description if the frame is invalid or its
/// checksum does not verify.
pub fn decode_last_applied(raw: &[u8]) -> Result<u64, String> {
    let (header, _) = decode_wal_record(raw).map_err(|e| e.to_string())?;
    Ok(header.lsn)
}

/// Encode an expanded op-set as a compact binary WAL payload: an op count
/// followed by `(tag, key, [value])` entries. This is the log-first WAL's
/// record content (decision B): the log stores state transitions, so recovery
/// applies them directly and needs no expander.
///
/// # Panics
/// Panics if an op-set has more than `u32::MAX` ops or a key/value exceeds
/// `u32::MAX` bytes, which the 64 MiB record ceiling makes impossible.
#[must_use]
pub fn encode_op_set(ops: &[Op]) -> Vec<u8> {
    let mut out = Vec::with_capacity(op_set_encoded_len(ops));
    write_op_set(&mut out, ops);
    out
}

/// Append the op-set encoding of `ops` to `out`.
fn write_op_set(out: &mut Vec<u8>, ops: &[Op]) {
    let count = u32::try_from(ops.len()).expect("op count fits u32");
    out.extend_from_slice(&count.to_le_bytes());
    for op in ops {
        encode_one_op(out, op);
    }
}

/// Exact byte length [`encode_op_set`] would produce for `ops`, computed
/// without allocating — used for cheap pre-commit size checks (deciding
/// whether a commit needs chaining, or exceeds the configured transaction
/// ceiling entirely) without building the buffer just to measure it.
#[must_use]
pub fn op_set_encoded_len(ops: &[Op]) -> usize {
    let mut len = 4; // count prefix
    for op in ops {
        len += 1; // tag
        len += match op {
            Op::Put(key, value) | Op::Set(key, value) => 4 + key.len() + 4 + value.len(),
            Op::Del(key) | Op::Purge(key) => 4 + key.len(),
        };
    }
    len
}

fn put_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("field length fits u32");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

fn take_u32(cur: &mut &[u8]) -> Result<u32, String> {
    if cur.len() < 4 {
        return Err("truncated op-set length".into());
    }
    let (head, rest) = cur.split_at(4);
    *cur = rest;
    Ok(u32::from_le_bytes(head.try_into().expect("4-byte slice")))
}

fn take_bytes(cur: &mut &[u8]) -> Result<Vec<u8>, String> {
    let len = take_u32(cur)? as usize;
    if cur.len() < len {
        return Err("truncated op-set field".into());
    }
    let (head, rest) = cur.split_at(len);
    *cur = rest;
    Ok(head.to_vec())
}

/// Encode one op's tag + length-prefixed fields — the per-op body
/// [`encode_op_set`]'s loop emits once per op.
fn encode_one_op(out: &mut Vec<u8>, op: &Op) {
    match op {
        Op::Put(key, value) => {
            out.push(0);
            put_len_prefixed(out, key);
            put_len_prefixed(out, value);
        }
        Op::Del(key) => {
            out.push(1);
            put_len_prefixed(out, key);
        }
        Op::Set(key, value) => {
            out.push(2);
            put_len_prefixed(out, key);
            put_len_prefixed(out, value);
        }
        Op::Purge(key) => {
            out.push(3);
            put_len_prefixed(out, key);
        }
    }
}

/// Decode one op's tag + length-prefixed fields, advancing `cur` past it —
/// the per-op body [`decode_op_set`]'s loop consumes once per op.
fn decode_one_op(cur: &mut &[u8]) -> Result<Op, String> {
    let tag = *cur.first().ok_or("truncated op tag")?;
    *cur = &cur[1..];
    let key = take_bytes(cur)?;
    Ok(match tag {
        0 => Op::Put(key, take_bytes(cur)?),
        1 => Op::Del(key),
        2 => Op::Set(key, take_bytes(cur)?),
        3 => Op::Purge(key),
        other => return Err(format!("unknown op tag {other}")),
    })
}

/// Decode an op-set encoded by [`encode_op_set`].
///
/// # Errors
/// Returns a human-readable description if the payload is truncated, carries
/// an unknown op tag, or has trailing bytes.
pub fn decode_op_set(raw: &[u8]) -> Result<Vec<Op>, String> {
    let mut cur = raw;
    let count = take_u32(&mut cur)? as usize;
    let mut ops = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        ops.push(decode_one_op(&mut cur)?);
    }
    if !cur.is_empty() {
        return Err("trailing bytes in op set".into());
    }
    Ok(ops)
}

/// Record-kind tag distinguishing a bare op-set from one paired with its
/// originating command. A one-byte prefix inside the framed
/// payload, so a WAL (or an export stream) can carry a mix of both kinds and
/// decode each unambiguously — low-level/raw op application (tests, the
/// full-replay shadow-map path) never had a `CommitBatch` to begin with and
/// keeps using `None`.
const OPS_RECORD_KIND_BARE: u8 = 0;
const OPS_RECORD_KIND_WITH_COMMAND: u8 = 1;
/// A non-final fragment of a chained commit (a large
/// transaction): this LSN's commit did not fit in one 64 MiB framed record, so
/// its *logical body* — the same `[op_set_len][op_set][command?]` layout
/// [`encode_ops_record`]'s `WITH_COMMAND` kind uses — is split across
/// several records sharing one LSN, at arbitrary raw byte boundaries (never
/// aligned to op or command boundaries, since nothing is decoded until every
/// fragment is reassembled). Reconstructed only once the matching
/// [`OPS_RECORD_KIND_CHAIN_FINAL`] is seen (see [`ChainAssembler`]).
const OPS_RECORD_KIND_CHAIN_CONTINUE: u8 = 2;
/// The final fragment of a chained commit: like `CHAIN_CONTINUE`, but marks
/// the chain complete — this is the point at which the concatenation of
/// every fragment's bytes becomes a decodable logical body.
const OPS_RECORD_KIND_CHAIN_FINAL: u8 = 3;

/// Encode an expanded op-set as a framed WAL record at `(shard_id, lsn)`,
/// optionally paired with the serialized `CommitBatch` that produced it
/// (carrying the command in the WAL record is what makes the
/// command journal — `commit_key` per LSN — fully derivable from the WAL,
/// closing the `__log__` redb row that used to be the only source of it).
/// `command_payload` is the batch pre-serialized by the caller (already
/// computed once for the outcome-payload row), not re-serialized here.
///
/// # Panics
/// Panics if the op-set encodes to more than `u32::MAX` bytes, which the
/// 64 MiB record ceiling makes impossible.
#[must_use]
pub fn encode_ops_record(
    shard_id: u32,
    lsn: u64,
    ops: &[Op],
    command_payload: Option<&[u8]>,
) -> Vec<u8> {
    // Encoded once, into one exactly-sized buffer: the header is written
    // with a placeholder CRC, the body is appended in place, and the length
    // and checksum are patched at the end (SD-7.3).
    let op_set_len = op_set_encoded_len(ops);
    let body_len = 1 + command_payload.map_or(op_set_len, |command| 4 + op_set_len + command.len());
    let payload_len = u32::try_from(body_len).expect("payload length fits u32");
    let mut out = Vec::with_capacity(WAL_HEADER_LEN + body_len);
    out.extend_from_slice(&WAL_MAGIC);
    out.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&shard_id.to_le_bytes());
    out.extend_from_slice(&lsn.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&[0; 4]); // CRC, patched below
    if let Some(command) = command_payload {
        out.push(OPS_RECORD_KIND_WITH_COMMAND);
        out.extend_from_slice(
            &u32::try_from(op_set_len)
                .expect("op-set fits u32")
                .to_le_bytes(),
        );
        write_op_set(&mut out, ops);
        out.extend_from_slice(command);
    } else {
        out.push(OPS_RECORD_KIND_BARE);
        write_op_set(&mut out, ops);
    }
    debug_assert_eq!(out.len(), WAL_HEADER_LEN + body_len);
    let crc_at = WAL_HEADER_LEN - 4;
    let mut hasher = Crc32::new();
    hasher.update(&out[..crc_at]);
    hasher.update(&out[WAL_HEADER_LEN..]);
    out[crc_at..WAL_HEADER_LEN].copy_from_slice(&hasher.finalize().to_le_bytes());
    out
}

/// Decode a framed op-set record, verifying the frame checksum. The returned
/// header carries the routing shard and LSN; the `CommitBatch` is `Some` only
/// for a record encoded with one (every real client commit through the
/// normal publish path — see [`encode_ops_record`]).
///
/// # Errors
/// Returns a human-readable description if the frame is invalid, the record
/// kind is unrecognized, or the payload is not a valid op-set/commit batch.
///
/// # Panics
/// Panics only if an internal fixed-width slice conversion fails after its
/// length has been bounds-checked, which cannot happen.
pub fn decode_ops_record(raw: &[u8]) -> Result<(WalHeader, Vec<Op>, Option<CommitBatch>), String> {
    let (header, payload) = decode_wal_record(raw).map_err(|e| e.to_string())?;
    let (&kind, rest) = payload.split_first().ok_or("empty WAL record payload")?;
    match kind {
        OPS_RECORD_KIND_BARE => Ok((header, decode_op_set(rest)?, None)),
        OPS_RECORD_KIND_WITH_COMMAND => {
            if rest.len() < 4 {
                return Err("truncated combined WAL record".into());
            }
            let op_set_len =
                u32::from_le_bytes(rest[0..4].try_into().expect("4-byte slice")) as usize;
            if rest.len() < 4 + op_set_len {
                return Err("truncated combined WAL record op-set".into());
            }
            let ops = decode_op_set(&rest[4..4 + op_set_len])?;
            let batch: CommitBatch = rushdb_types::command_codec::decode(&rest[4 + op_set_len..])
                .map_err(|e| format!("invalid commit batch: {e}"))?;
            Ok((header, ops, Some(batch)))
        }
        other => Err(format!("unknown WAL record kind {other}")),
    }

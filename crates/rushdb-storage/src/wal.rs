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
}

/// Split `ops` (optionally paired with `command_payload`) into one or more
/// framed WAL records at `(shard_id, lsn)`, each a complete, independently
/// checksummed frame whose total length never exceeds `max_record_bytes`.
/// All returned records share one LSN; only the last is tagged
/// [`OPS_RECORD_KIND_CHAIN_FINAL`] (or, when everything already fits in one
/// record — overwhelmingly the common case — the single returned record is
/// byte-identical to [`encode_ops_record`]'s ordinary `BARE`/`WITH_COMMAND`
/// output). When chaining is needed, the logical body (op-set plus optional
/// command, laid out exactly like `WITH_COMMAND`'s own format) is cut at
/// arbitrary raw byte boundaries — never aligned to op or command
/// boundaries — which is what lets a command payload of any size be chained
/// too, not just the op-set: nothing is decoded fragment-by-fragment, only
/// once every fragment of the chain is reassembled (see [`ChainAssembler`]).
///
/// # Panics
/// Panics if `max_record_bytes` is too small to fit even the frame header
/// and kind byte — a caller-side configuration error, not a data-dependent
/// runtime condition.
#[must_use]
pub fn encode_chained_ops_record(
    shard_id: u32,
    lsn: u64,
    ops: &[Op],
    command_payload: Option<&[u8]>,
    max_record_bytes: usize,
) -> Vec<Vec<u8>> {
    let whole_len = WAL_HEADER_LEN
        + 1
        + command_payload.map_or(op_set_encoded_len(ops), |command| {
            4 + op_set_encoded_len(ops) + command.len()
        });
    if whole_len <= max_record_bytes {
        return vec![encode_ops_record(shard_id, lsn, ops, command_payload)];
    }

    // The logical body to chain: identical layout to `WITH_COMMAND`'s own
    // body (op_set_len-prefixed op-set, then command bytes — possibly
    // empty, when there is no command). Reusing this exact layout means
    // reassembly can reuse `WITH_COMMAND`'s own parsing, not a new format.
    let op_set = encode_op_set(ops);
    let op_set_len = u32::try_from(op_set.len()).expect("op-set fits u32");
    let mut logical_body =
        Vec::with_capacity(4 + op_set.len() + command_payload.map_or(0, <[u8]>::len));
    logical_body.extend_from_slice(&op_set_len.to_le_bytes());
    logical_body.extend_from_slice(&op_set);
    if let Some(command) = command_payload {
        logical_body.extend_from_slice(command);
    }

    let chunk_budget = max_record_bytes
        .checked_sub(WAL_HEADER_LEN + 1)
        .expect("max_record_bytes covers at least the frame header and kind byte");
    assert!(
        chunk_budget > 0,
        "max_record_bytes ({max_record_bytes}) leaves no room for any payload"
    );

    let chunks: Vec<&[u8]> = logical_body.chunks(chunk_budget).collect();
    let last_index = chunks.len().saturating_sub(1);
    chunks
        .into_iter()
        .enumerate()
        .map(|(i, chunk)| {
            let kind = if i == last_index {
                OPS_RECORD_KIND_CHAIN_FINAL
            } else {
                OPS_RECORD_KIND_CHAIN_CONTINUE
            };
            let mut body = Vec::with_capacity(1 + chunk.len());
            body.push(kind);
            body.extend_from_slice(chunk);
            encode_wal_record(shard_id, lsn, &body)
        })
        .collect()
}

/// Reconstructs a (possibly chained) commit from consecutive framed WAL
/// records, fed one at a time in physical log order. The overwhelmingly
/// common case — a `BARE`/`WITH_COMMAND` record — reconstructs immediately.
/// A chained commit's `CHAIN_CONTINUE` fragments are buffered until the
/// matching `CHAIN_FINAL` is fed, at which point the whole op-set is
/// returned — in full, exactly once, never partially. This is the one
/// reconstruction implementation crash recovery and replication apply both
/// use, so there is never a second, potentially drifting copy of this logic.
#[derive(Default)]
pub struct ChainAssembler {
    open: Option<OpenChain>,
}

struct OpenChain {
    lsn: u64,
    shard_id: u32,
    /// Byte offset (in whatever stream the caller is scanning) where this
    /// chain's first fragment began — what recovery must truncate back to,
    /// and what retention must never truncate through, if the chain never
    /// completes.
    start_offset: u64,
    /// Concatenated raw logical-body bytes from every `CHAIN_CONTINUE` fed
    /// so far, in order.
    fragments: Vec<u8>,
}

/// Result of feeding one record to a [`ChainAssembler`].
pub enum ChainFeed {
    /// This LSN's commit's *framing* is fully reconstructed: `header` plus
    /// the logical body (`[op_set_len:4][op_set][command bytes, possibly
    /// empty]` — exactly `WITH_COMMAND`'s own body shape), ready to decode
    /// via [`decode_logical_body`]. Decoding is a separate, explicit step
    /// — deliberately never done inside [`ChainAssembler::feed`] — so a
    /// *content* corruption (e.g. an unparsable embedded `CommitBatch`,
    /// `metadata_retirement_rejects_a_corrupt_wal_command_envelope`'s exact
    /// case) is never mistaken for a torn write and silently truncated by a
    /// caller that only wants structural chain-completeness (`read_segment`'s
    /// recovery pass checks `is_open()`/`open_start_offset()` and never
    /// calls `decode_logical_body` at all).
    Complete(WalHeader, Vec<u8>),
    /// More fragments are still expected for this LSN; nothing to decode yet.
    Pending,
}

/// Decode a [`ChainFeed::Complete`] logical body into its op-set and
/// optional command. Kept separate from [`ChainAssembler::feed`] (see
/// [`ChainFeed::Complete`]'s doc) so framing/chain-structure completeness
/// and content validity are never conflated.
///
/// # Errors
/// Returns a human-readable description if the body is truncated, the
/// op-set carries an unknown tag, or a non-empty command fails to parse as
/// a `CommitBatch`.
///
/// # Panics
/// Panics only if an internal fixed-width slice conversion fails after its
/// length has been bounds-checked, which cannot happen.
pub fn decode_logical_body(
    header: WalHeader,
    body: &[u8],
) -> Result<(WalHeader, Vec<Op>, Option<CommitBatch>), String> {
    if body.len() < 4 {
        return Err("truncated reassembled chain body".into());
    }
    let op_set_len = u32::from_le_bytes(body[0..4].try_into().expect("4-byte slice")) as usize;
    if body.len() < 4 + op_set_len {
        return Err("truncated reassembled chain op-set".into());
    }
    let ops = decode_op_set(&body[4..4 + op_set_len])?;
    let command_bytes = &body[4 + op_set_len..];
    let batch = if command_bytes.is_empty() {
        None
    } else {
        Some(
            rushdb_types::command_codec::decode(command_bytes)
                .map_err(|e| format!("invalid commit batch: {e}"))?,
        )
    };
    Ok((header, ops, batch))
}

impl ChainAssembler {
    /// Feed one decoded, checksum-verified framed record, at `offset` in
    /// whatever byte stream the caller is scanning (the segment file for
    /// recovery, the replication page stream for a follower). Purely
    /// structural: never decodes the op-set or command (see
    /// [`ChainFeed::Complete`]'s doc for why).
    ///
    /// # Errors
    /// Returns an error if a `CHAIN_CONTINUE`/`CHAIN_FINAL` arrives for a
    /// different LSN than the currently-open chain, if a `BARE`/
    /// `WITH_COMMAND` record arrives while a chain is open, or if the frame
    /// itself fails to decode (bad magic/version/checksum) — all of which
    /// indicate a corrupt or reordered log, never a legitimate write
    /// pattern.
    pub fn feed(&mut self, raw: &[u8], offset: u64) -> Result<ChainFeed, String> {
        let (header, payload) = decode_wal_record(raw).map_err(|e| e.to_string())?;
        let (&kind, rest) = payload.split_first().ok_or("empty WAL record payload")?;
        match kind {
            OPS_RECORD_KIND_BARE => {
                if self.open.is_some() {
                    return Err(format!(
                        "non-chained record at lsn {} arrived while a chain was open",
                        header.lsn
                    ));
                }
                // Normalize to the `WITH_COMMAND`-shaped logical body (an
                // explicit op_set_len prefix, empty trailing command) so
                // every `Complete` result shares one decodable layout
                // regardless of which kind produced it.
                let op_set_len =
                    u32::try_from(rest.len()).map_err(|_| "op-set too large".to_string())?;
                let mut body = Vec::with_capacity(4 + rest.len());
                body.extend_from_slice(&op_set_len.to_le_bytes());
                body.extend_from_slice(rest);
                Ok(ChainFeed::Complete(header, body))
            }
            OPS_RECORD_KIND_WITH_COMMAND => {
                if self.open.is_some() {
                    return Err(format!(
                        "non-chained record at lsn {} arrived while a chain was open",
                        header.lsn
                    ));
                }
                // `rest` is already exactly the target logical-body shape.
                Ok(ChainFeed::Complete(header, rest.to_vec()))
            }
            OPS_RECORD_KIND_CHAIN_CONTINUE => {
                match &mut self.open {
                    Some(open) if open.lsn == header.lsn && open.shard_id == header.shard_id => {
                        open.fragments.extend_from_slice(rest);
                    }
                    Some(open) => {
                        return Err(format!(
                            "chain continuation at (lsn {}, shard {}) arrived while (lsn {}, shard {}) was open",
                            header.lsn, header.shard_id, open.lsn, open.shard_id
                        ));
                    }
                    None => {
                        self.open = Some(OpenChain {
                            lsn: header.lsn,
                            shard_id: header.shard_id,
                            start_offset: offset,
                            fragments: rest.to_vec(),
                        });
                    }
                }
                Ok(ChainFeed::Pending)
            }
            OPS_RECORD_KIND_CHAIN_FINAL => {
                if let Some(open) = &self.open {
                    if open.lsn != header.lsn || open.shard_id != header.shard_id {
                        return Err(format!(
                            "chain final at (lsn {}, shard {}) arrived while (lsn {}, shard {}) was open",
                            header.lsn, header.shard_id, open.lsn, open.shard_id
                        ));
                    }
                }
                let mut logical_body = self.open.take().map_or_else(Vec::new, |o| o.fragments);
                logical_body.extend_from_slice(rest);
                Ok(ChainFeed::Complete(header, logical_body))
            }
            other => Err(format!("unknown WAL record kind {other}")),
        }
    }

    /// True while a chain is open (fragments buffered, final not yet seen).
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.open.is_some()
    }

    /// The LSN of the currently-open chain, if any.
    #[must_use]
    pub fn open_lsn(&self) -> Option<u64> {
        self.open.as_ref().map(|o| o.lsn)
    }

    /// The byte offset where the currently-open chain's first fragment
    /// began, if any — what a caller reaching end-of-stream with an open
    /// chain must truncate back to (discarding the whole incomplete chain,
    /// never partially applying it).
    #[must_use]
    pub fn open_start_offset(&self) -> Option<u64> {
        self.open.as_ref().map(|o| o.start_offset)
    }
}

/// A published durability checkpoint: the durable LSN (header), the GC floor,
/// and the wall-clock time the checkpoint was published. The record's frame
/// checksum makes a torn/partial checkpoint fail verification on load.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    /// Physical shard this checkpoint belongs to.
    pub shard_id: u32,
    /// Durable LSN covered by this checkpoint.
    pub durable_lsn: u64,
    /// GC floor that must remain readable.
    pub gc_watermark: u64,
    /// Wall-clock time the checkpoint was published, in ms since epoch.
    pub published_at_ms: u64,
}

/// Encode a checkpoint as a framed WAL record (LSN carried in the header,
/// floor + timestamp in the payload).
#[must_use]
pub fn encode_checkpoint(checkpoint: &Checkpoint) -> Vec<u8> {
    let mut body = Vec::with_capacity(16);
    body.extend_from_slice(&checkpoint.gc_watermark.to_le_bytes());
    body.extend_from_slice(&checkpoint.published_at_ms.to_le_bytes());
    encode_wal_record(checkpoint.shard_id, checkpoint.durable_lsn, &body)
}

/// Decode a checkpoint from a framed WAL record.
///
/// # Errors
/// Returns a human-readable description if the frame is invalid or its
/// payload is not exactly the expected 16 bytes.
pub fn decode_checkpoint(raw: &[u8]) -> Result<Checkpoint, String> {
    let (header, body) = decode_wal_record(raw).map_err(|e| e.to_string())?;
    if body.len() != 16 {
        return Err(format!("invalid checkpoint body length: {}", body.len()));
    }
    let mut gc = [0u8; 8];
    let mut ts = [0u8; 8];
    gc.copy_from_slice(&body[0..8]);
    ts.copy_from_slice(&body[8..16]);
    Ok(Checkpoint {
        shard_id: header.shard_id,
        durable_lsn: header.lsn,
        gc_watermark: u64::from_le_bytes(gc),
        published_at_ms: u64::from_le_bytes(ts),
    })
}

/// Framed-stream writer for export/backup: emits `{len}{crc}{bytes}` chunks so
/// a consumer can detect truncation/corruption without reading the whole
/// artifact. (P17 foundations.)
///
/// # Errors
/// Returns an I/O error if the chunk length exceeds `u32::MAX` or writing to
/// the sink fails.
pub fn write_framed_chunk<W: io::Write>(writer: &mut W, bytes: &[u8]) -> io::Result<()> {
    let mut hasher = Crc32::new();
    hasher.update(bytes);
    let len = u32::try_from(bytes.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "framed chunk exceeds u32 length",
        )
    })?;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&hasher.finalize().to_le_bytes())?;
    writer.write_all(bytes)
}

/// Read one framed chunk written by [`write_framed_chunk`].
///
/// # Errors
/// Returns an I/O error if reading fails, the chunk exceeds the 64 MiB
/// ceiling, or its checksum does not match. A clean end of stream yields
/// `Ok(None)`.
pub fn read_framed_chunk<R: io::Read>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 64 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "framed chunk exceeds 64 MiB",
        ));
    }
    let mut crc_buf = [0u8; 4];
    reader.read_exact(&mut crc_buf)?;
    let stored_crc = u32::from_le_bytes(crc_buf);
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload)?;
    let mut hasher = Crc32::new();
    hasher.update(&payload);
    if hasher.finalize() != stored_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "framed chunk checksum mismatch",
        ));
    }
    Ok(Some(payload))
}

/// Manifest describing one exported WAL segment range. Written first in a
/// segment stream so a consumer can validate the range before reading records.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WalSegmentManifest {
    /// Segment magic identifying the artifact type.
    pub magic: String,
    /// Framing format version.
    pub format_version: u32,
    /// Physical shard the segment was exported from.
    pub shard_id: u32,
    /// First LSN included in the segment.
    pub from_lsn: u64,
    /// Last LSN included in the segment.
    pub to_lsn: u64,
    /// Number of framed records that follow.
    pub record_count: u64,
    /// CRC32 over the concatenated framed records that follow.
    pub records_crc: u32,
}

/// Magic identifying a WAL segment manifest.
pub const SEGMENT_MAGIC: &str = "RDBWALSEG";

/// Framed snapshot export magic: distinct from the WAL segment magic so a
/// restore can reject a mismatched artifact type immediately.
pub const SNAPSHOT_MAGIC: &str = "RDBWALSNAP";

/// Manifest header for a framed snapshot stream (W6, D9/P17). Written as the
/// first framed chunk; consumers validate `magic`/`format_version` before
/// reading tenant chunks. `tenants_crc` covers the concatenated tenant chunks,
/// so a torn/corrupted stream is rejected at restore time.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotManifest {
    /// Snapshot magic identifying the artifact type.
    pub magic: String,
    /// Framing format version.
    pub format_version: u32,
    /// Physical shard the snapshot was exported from.
    pub shard_id: u32,
    /// First LSN included in the snapshot.

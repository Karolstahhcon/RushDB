//! Retry receipts: one row per commit key answers "did this
//! commit already happen, and what did it return?" for
//! `idempotency-window-seconds`, then a bounded sweep removes it.
//!
//! `RECEIPT(uuid) → status ‖ ci ‖ created_ms ‖ payload` is the single
//! source of the outcome (it replaces the separate status and payload rows,
//! which were kept forever and pruned by commit count respectively);
//! `RECEIPT_EXP(created_ms, uuid)` orders receipts by age for the sweep. A
//! receipt older than the window is treated as absent even before the
//! sweep reaches it, so a late retry executes as a new commit.

use uuid::Uuid;

use crate::{OutcomeStatus, StorageError};

/// Both receipt families (storage accounting ranges over this).
pub(crate) const RECEIPT_FAMILY: &[u8] = b"receipt";
/// Receipt rows, keyed by commit key.
pub(crate) const RECEIPT_PREFIX: &[u8] = b"receipt/";
/// Expiry index, keyed by creation time then commit key.
pub(crate) const RECEIPT_EXP_PREFIX: &[u8] = b"receipt-exp/";

const STATUS_ABORTED: u8 = 0;
const STATUS_COMMITTED: u8 = 1;
/// Payload flag: the response body follows as stored.
const PAYLOAD_RAW: u8 = 1;
/// Payload flag: the response body follows zstd-compressed (a 500-document ingest response shrinks several-fold).
const PAYLOAD_ZSTD: u8 = 2;
/// zstd level for receipt payloads: fast, written once per commit.
const PAYLOAD_ZSTD_LEVEL: i32 = 3;
/// `status(1) + ci(8) + created_ms(8)`; a payload follows for commits that
/// stored one.
const HEADER_LEN: usize = 17;

pub(crate) fn receipt_key(commit_key: Uuid) -> Vec<u8> {
    let mut key = RECEIPT_PREFIX.to_vec();
    key.extend_from_slice(commit_key.as_bytes());
    key
}

pub(crate) fn expiry_key(created_ms: u64, commit_key: Uuid) -> Vec<u8> {
    let mut key = RECEIPT_EXP_PREFIX.to_vec();
    key.extend_from_slice(&created_ms.to_be_bytes());
    key.extend_from_slice(commit_key.as_bytes());
    key
}

/// Commit key of an expiry-index row.
pub(crate) fn expiry_commit_key(key: &[u8]) -> Result<(u64, Uuid), StorageError> {
    let rest = key
        .strip_prefix(RECEIPT_EXP_PREFIX)
        .filter(|rest| rest.len() == 24)
        .ok_or(StorageError::CorruptLog)?;
    let created = u64::from_be_bytes(rest[..8].try_into().map_err(|_| StorageError::CorruptLog)?);
    let id = Uuid::from_slice(&rest[8..]).map_err(|_| StorageError::CorruptLog)?;
    Ok((created, id))
}

/// A decoded receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Receipt {
    pub(crate) status: OutcomeStatus,
    pub(crate) commit_index: u64,
    pub(crate) created_ms: u64,
    /// The stored response body; `None` for aborts and for commits applied
    /// without one (command replay, replication without a payload).
    pub(crate) payload: Option<Vec<u8>>,
}

impl Receipt {
    /// Whether the receipt is past the idempotency window at `now_ms`.
    pub(crate) fn expired(&self, now_ms: u64, window_ms: u64) -> bool {
        now_ms.saturating_sub(self.created_ms) > window_ms
    }

//! Fallible LMPG primitives over storage format v3. Corrupt rows are
//! errors, not missing records. Eager adjacency remains subject to the Txn
//! read cap.
use super::{
    keys_ext::{
        adj_pair_prefix, adj_prefix_typed, definition_row_key, family_key, rel_by_ord_key,
        rel_row_key,
    },
    TryRead,
};
use crate::derived;
use rushdb_storage::StorageError;
use rushdb_types::ids::{OwnerKind, RecordId, RelationshipId, TenantId};
use rushdb_types::keys::{self as k, key, u64be};
use rushdb_types::occurrence::{
    decode_occ_value, OccurrenceRow, OccurrenceValue, PropertyDefinition, PropertyFamily,
    RecordHeader, RelationshipHeader,
};

/// An adjacency edge: `(relationship type, other node ordinal, relationship ordinal)`.
pub type Edge = (String, u64, u64);

fn decode<T: serde::de::DeserializeOwned>(raw: Option<Vec<u8>>) -> Result<Option<T>, StorageError> {
    raw.map(|raw| serde_json::from_slice(&raw).map_err(|_| StorageError::CorruptLog))
        .transpose()
}

/// One canonical occurrence: identity `(kind, owner, pid, elem)` plus
/// its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Occurrence {
    /// Owner kind.
    pub kind: OwnerKind,
    /// Owner ordinal.
    pub owner: u64,
    /// Property id.
    pub pid: u64,
    /// Element slot (`0` for scalars).
    pub elem: u64,
    /// Whether the occurrence is an array element.
    pub array: bool,
    /// Value state.
    pub value: OccurrenceValue,
    /// Order-preserving payload of a stored scalar (empty otherwise).
    pub payload: Vec<u8>,
}

impl Occurrence {
    fn decode(key: &[u8], raw: &[u8]) -> Result<Self, StorageError> {
        let (_, kind, owner, pid, elem) = k::parse_occ_key(key).ok_or(StorageError::CorruptLog)?;
        let value = decode_occ_value(raw).map_err(|_| StorageError::CorruptLog)?;
        Ok(Self {
            kind,
            owner,
            pid,
            elem,
            array: value.array,
            value: value.value,
            payload: value.payload.to_vec(),
        })
    }

    /// Canonical value hash.
    #[must_use]
    pub fn value_hash(&self) -> u64 {
        match self.value {
            OccurrenceValue::Stored { ty } => rushdb_types::hash::value_hash(
                rushdb_types::encoding::logical_tag(ty),
                &self.payload,
            ),
            OccurrenceValue::ExplicitNull | OccurrenceValue::EmptyArray => {
                rushdb_types::hash::null_value_hash()
            }
        }
    }

    /// The command-vocabulary row for this occurrence.
    #[must_use]
    pub fn to_row(&self, family_id: u64) -> OccurrenceRow {
        OccurrenceRow {
            owner_kind: self.kind,
            owner_ord: self.owner,
            property_id: self.pid,
            family_id,
            elem_ordinal: self.array.then_some(self.elem),
            occurrence_value: self.value,
        }
    }
}

fn collect_occurrences<R: TryRead + ?Sized>(
    r: &R,
    prefix: &[u8],
) -> Result<Vec<Occurrence>, StorageError> {
    r.range_prefix(prefix)?
        .into_iter()
        .map(|(key, raw)| Occurrence::decode(&key, &raw))
        .collect()
}

/// Every occurrence of one owner, in `(pid, elem)` order: one prefix scan.
///
/// # Errors
/// Returns [`StorageError`] if the scan fails or a row is corrupt.
pub fn occurrences_of_owner<R: TryRead + ?Sized>(
    r: &R,
    t: TenantId,
    kind: OwnerKind,
    owner: u64,
) -> Result<Vec<Occurrence>, StorageError> {
    collect_occurrences(r, &k::occ_owner_prefix(t, kind, owner))
}

/// Every element of one owner's pid, in element order.
///
/// # Errors
/// Returns [`StorageError`] if the scan fails or a row is corrupt.
pub fn occurrences_of_pid<R: TryRead + ?Sized>(
    r: &R,
    t: TenantId,
    kind: OwnerKind,
    owner: u64,
    pid: u64,
) -> Result<Vec<Occurrence>, StorageError> {
    collect_occurrences(r, &k::occ_pid_prefix(t, kind, owner, pid))
}

/// One occurrence slot.
///
/// # Errors
/// Returns [`StorageError`] if the read fails or the row is corrupt.
pub fn occurrence<R: TryRead + ?Sized>(
    r: &R,
    t: TenantId,
    kind: OwnerKind,
    owner: u64,
    pid: u64,
    elem: u64,
) -> Result<Option<Occurrence>, StorageError> {
    let occ_key = k::occ_key(t, kind, owner, pid, elem);
    r.get(&occ_key)?
        .map(|raw| Occurrence::decode(&occ_key, &raw))
        .transpose()
}

/// Looks up a record header by public id: `RECORD_ID` then `RECORD_HDR`.
///
/// # Errors
/// Returns [`StorageError`] if a read fails or a row is corrupt.
pub fn record_by_id<R: TryRead + ?Sized>(
    r: &R,
    t: TenantId,
    id: &RecordId,
) -> Result<Option<RecordHeader>, StorageError> {
    let Some(raw) = r.get(&k::record_id_key(t, &id.0))? else {
        return Ok(None);
    };
    let ordinal = u64::from_be_bytes(raw.try_into().map_err(|_| StorageError::CorruptLog)?);
    record_by_ord(r, t, ordinal)
}

/// Looks up a record header by storage ordinal (one point read).
///
/// # Errors
/// Returns [`StorageError`] if the read fails or the row is corrupt.
pub fn record_by_ord<R: TryRead + ?Sized>(
    r: &R,
    t: TenantId,
    ord: u64,
) -> Result<Option<RecordHeader>, StorageError> {
    let Some(raw) = r.get(&k::record_hdr_key(t, ord))? else {
        return Ok(None);
    };
    let (record_id, label) = k::decode_record_hdr(&raw).ok_or(StorageError::CorruptLog)?;
    Ok(Some(RecordHeader {
        record_id,
        ordinal: ord,
        label: label.to_owned(),
    }))
}

/// Looks up a property family by family id.
///
/// # Errors
/// Returns [`StorageError`] if the read fails or the row is corrupt.
pub fn family<R: TryRead + ?Sized>(
    r: &R,
    t: TenantId,
    id: u64,
) -> Result<Option<PropertyFamily>, StorageError> {
    decode(r.get(&family_key(t, id))?)
}

/// Looks up a property definition row by property id.
///
/// # Errors
/// Returns [`StorageError`] if the read fails or the row is corrupt.
pub fn definition<R: TryRead + ?Sized>(
    r: &R,
    t: TenantId,
    id: u64,
) -> Result<Option<PropertyDefinition>, StorageError> {
    decode(r.get(&definition_row_key(t, id))?)

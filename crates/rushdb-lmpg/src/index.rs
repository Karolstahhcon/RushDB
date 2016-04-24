//! Declared ordered indexes: definitions, expansion of the index
//! mutations, apply-time maintenance of `INDEX_ENTRY` rows, and the read API
//! the planner uses.
//!
//! Entries are derived rows. For an index `I` and a record `r`, the
//! entries are `{(value, r) : value ∈ OCC(r, pids(I.property))}` when `r` has
//! `I.label` and `I.state` covers `r`'s ordinal, and nothing otherwise. Every
//! commit recomputes that set for exactly the `(I, r)` pairs it can change —
//! records whose occurrences or header changed, and records whose coverage
//! moved with `I`'s cursor — so entries always equal a from-scratch
//! derivation of the base rows, during a build or drop as well.

use std::collections::{BTreeMap, BTreeSet};

use rushdb_storage::{DeriveRead, StorageError, Txn};
use rushdb_types::command::Mutation;
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::index::{IndexDef, IndexKind, IndexState, TEXT_ALL_PROPERTIES};
use rushdb_types::keys::{self as k, key};
use rushdb_types::model::{LogicalType, TypeState};
use rushdb_types::occurrence::{decode_occ_value, OccurrenceValue};

use crate::reader::{keys_ext, TryRead};

fn index_seq_key(t: TenantId) -> Vec<u8> {
    key(t, &[b"meta", b"index-seq"])
}

fn decode_def(raw: &[u8]) -> Result<IndexDef, StorageError> {
    serde_json::from_slice(raw).map_err(|_| StorageError::CorruptLog)
}

fn encode_def(def: &IndexDef) -> Vec<u8> {
    serde_json::to_vec(def).expect("index definitions serialize")
}

fn builder(message: impl Into<String>) -> StorageError {
    StorageError::Builder(message.into())
}

/// Smallest structural embedding.
pub const STRUCTURAL_MIN_DIMENSIONS: u32 = 8;
/// Largest structural embedding.
pub const STRUCTURAL_MAX_DIMENSIONS: u32 = 4096;
/// A record with more relationships than this is a hub: a change next to
/// it does not mark its neighbours one by one; the structural indexes of
/// its tenant are rebuilt instead (every member marked again).
pub const STRUCTURAL_HUB_DEGREE: usize = 10_000;

fn def_by_id(txn: &Txn, t: TenantId, id: u32) -> Result<IndexDef, StorageError> {
    let raw = txn
        .get(&k::index_def_key(t, id))?
        .ok_or_else(|| builder(format!("index {id} does not exist")))?;
    decode_def(&raw)
}

fn id_by_name(txn: &Txn, t: TenantId, name: &str) -> Result<Option<u32>, StorageError> {
    txn.get(&k::index_name_key(t, name))?
        .map(|raw| {
            <[u8; 4]>::try_from(raw.as_slice())
                .map(u32::from_be_bytes)
                .map_err(|_| StorageError::CorruptLog)
        })
        .transpose()
}

/// Expand one index mutation.
///
/// # Errors
/// Returns [`StorageError::Builder`] for a duplicate or unknown name, an
/// empty label/property, or a transition the current state does not allow.
pub(crate) fn expand(
    t: TenantId,
    ci: u64,
    m: &Mutation,
    txn: &mut Txn,
) -> Result<(), StorageError> {
    match m {
        Mutation::DefineIndex {
            name,
            label,
            property,
            kind,
        } => define(t, name, label, property, *kind, txn),
        Mutation::IndexBackfill { id, through } => backfill(t, ci, *id, *through, txn),
        Mutation::DropIndex { name } => {
            let id = id_by_name(txn, t, name)?
                .ok_or_else(|| builder(format!("index {name} does not exist")))?;
            let mut def = def_by_id(txn, t, id)?;
            if !matches!(def.state, IndexState::Dropping { .. }) {
                def.state = IndexState::Dropping { cursor: 0 };
                txn.put(k::index_def_key(t, id), encode_def(&def));
            }
            Ok(())
        }
        Mutation::IndexDrop { id, through } => drop_step(t, *id, *through, txn),
        Mutation::IndexApplied {
            index,
            upto,
            owners,
        } => {
            for owner in owners {
                let pending = k::index_pending_key(t, *index, *owner);
                let Some(raw) = txn.get(&pending)? else {
                    continue;
                };
                let marked = <[u8; 8]>::try_from(raw.as_slice())
                    .map(u64::from_be_bytes)
                    .map_err(|_| StorageError::CorruptLog)?;
                // A change committed after the applier's snapshot stays
                // pending.
                if marked <= *upto {
                    txn.del(pending);
                }
            }
            Ok(())
        }
        _ => Err(builder("not an index mutation")),
    }
}

fn define(
    t: TenantId,
    name: &str,
    label: &str,
    property: &str,
    kind: IndexKind,
    txn: &mut Txn,
) -> Result<(), StorageError> {
    let property = if property == TEXT_ALL_PROPERTIES {
        if kind == IndexKind::Ordered {
            return Err(builder("an ordered index covers one named property"));
        }
        property.to_owned()
    } else if let IndexKind::Structural { .. } = kind {
        return Err(builder(format!(
            "a structural index reads topology: its property is `{TEXT_ALL_PROPERTIES}`"
        )));
    } else {
        rushdb_types::value::normalize_name(property)
    };
    if let IndexKind::Structural { dimensions } = kind {
        if !(STRUCTURAL_MIN_DIMENSIONS..=STRUCTURAL_MAX_DIMENSIONS).contains(&dimensions) {
            return Err(builder(format!(
                "structural index dimensions must be {STRUCTURAL_MIN_DIMENSIONS}..={STRUCTURAL_MAX_DIMENSIONS}"
            )));
        }
    }
    if name.trim().is_empty() || label.trim().is_empty() || property.is_empty() {
        return Err(builder("an index needs a name, a label and a property"));
    }
    if id_by_name(txn, t, name)?.is_some() {
        return Err(builder(format!("index {name} already exists")));
    }
    let id = match txn.get(&index_seq_key(t))? {
        Some(raw) => <[u8; 4]>::try_from(raw.as_slice())
            .map(u32::from_be_bytes)
            .map_err(|_| StorageError::CorruptLog)?
            .checked_add(1)
            .ok_or_else(|| builder("index ids exhausted"))?,
        None => 1,
    };
    txn.put(index_seq_key(t), id.to_be_bytes().to_vec());
    txn.put(k::index_name_key(t, name), id.to_be_bytes().to_vec());
    txn.put(
        k::index_def_key(t, id),
        encode_def(&IndexDef {
            id,
            name: name.to_owned(),
            label: label.to_owned(),
            property,
            kind,
            state: IndexState::Building { cursor: 0 },
        }),
    );
    Ok(())
}

fn backfill(
    t: TenantId,
    ci: u64,
    id: u32,
    through: Option<u64>,
    txn: &mut Txn,
) -> Result<(), StorageError> {

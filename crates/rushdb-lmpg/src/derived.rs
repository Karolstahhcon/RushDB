//! Derived rows on disk (DC1/DC10/DC12, storage format v3).
//!
//! Base rows (`OCC`, `RECORD_HDR`, `LABEL`, adjacency, relationship rows and
//! their creation-time label groups) are the only stored truth. Everything a
//! query needs to count — presence, the per-property value dictionary,
//! counters, array markers — is derived from them by [`LmpgDeriver`]: one
//! deterministic function that turns a commit's base ops plus the state before
//! it into materialized derived rows. Storage applies those rows in the same
//! backend batch and at the same commit index as the base, never logs them,
//! and never puts them in a conflict set. Publication, command replay and WAL
//! replay (crash recovery, PITR, replicas) all call this one function, so every
//! node derives identical rows from the same base log.
//!
//! The read functions below return exact answers at the reader's snapshot
//! (pinned view, transaction overlay or write transaction).

use std::collections::{BTreeMap, BTreeSet};

use rushdb_storage::{DeriveRead, Deriver, Op, StorageError};
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{self as k, TENANT_PREFIX_LEN};
use rushdb_types::occurrence::{decode_occ_value, OccurrenceValue, RelationshipHeader};
use rushdb_types::stats::Histogram;

use crate::reader::{keys_ext, TryRead};

// ---------------------------------------------------------------------------
// Deriver
// ---------------------------------------------------------------------------

/// The LMPG apply-time deriver.
#[derive(Debug, Default, Clone, Copy)]
pub struct LmpgDeriver;

/// Base ops of one commit layered over the state before it.
struct After<'a> {
    ops: BTreeMap<&'a [u8], Option<&'a [u8]>>,
    before: &'a dyn DeriveRead,
}

impl<'a> After<'a> {
    fn new(ops: &'a [Op], before: &'a dyn DeriveRead) -> Self {
        let mut map = BTreeMap::new();
        for op in ops {
            match op {
                Op::Put(key, value) | Op::Set(key, value) => {
                    map.insert(key.as_slice(), Some(value.as_slice()));
                }
                Op::Del(key) | Op::Purge(key) => {
                    map.insert(key.as_slice(), None);
                }
            }
        }
        Self { ops: map, before }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        match self.ops.get(key) {
            Some(value) => Ok(value.map(<[u8]>::to_vec)),
            None => self.before.get(key),
        }
    }

    fn range_prefix(&self, prefix: &[u8]) -> Result<Rows, StorageError> {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> =
            self.before.range_prefix(prefix)?.into_iter().collect();
        for (key, value) in self.ops.range::<&[u8], _>(prefix..) {
            if !key.starts_with(prefix) {
                break;
            }
            match value {
                Some(value) => {
                    merged.insert(key.to_vec(), value.to_vec());
                }
                None => {
                    merged.remove(*key);
                }
            }
        }
        Ok(merged.into_iter().collect())
    }
}

/// `(key, value)` rows of a prefix scan.
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// A point read against one side (before or after) of a commit.
type Lookup<'a> = dyn Fn(&[u8]) -> Result<Option<Vec<u8>>, StorageError> + 'a;

impl DeriveRead for After<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        After::get(self, key)
    }

    fn range_prefix(&self, prefix: &[u8]) -> Result<Rows, StorageError> {
        After::range_prefix(self, prefix)
    }

    fn range(&self, start: &[u8], end: &[u8]) -> Result<Rows, StorageError> {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> =
            self.before.range(start, end)?.into_iter().collect();
        for (key, value) in &self.ops {
            if *key < start || (!end.is_empty() && *key >= end) {
                continue;
            }
            match value {
                Some(value) => {
                    merged.insert(key.to_vec(), value.to_vec());
                }
                None => {
                    merged.remove(*key);
                }
            }
        }
        Ok(merged.into_iter().collect())
    }
}

/// Owner presence state for one pid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Presence {
    Null,
    Value,
}

impl Presence {
    fn nullness(self) -> u8 {
        match self {
            Self::Null => k::NULLNESS_NULL,
            Self::Value => k::NULLNESS_VALUE,
        }
    }
}

/// Accumulated derived changes of one commit.
#[derive(Default)]
struct Changes {
    counters: BTreeMap<Vec<u8>, i64>,
    /// Refcounted value rows (dictionary entries and long-value digests),
    /// each with the distinct counter that tracks its non-zero rows.
    dict: BTreeMap<Vec<u8>, (Vec<u8>, i64)>,
    rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Changes {
    fn bump(&mut self, key: Vec<u8>, delta: i64) {
        if delta != 0 {
            *self.counters.entry(key).or_default() += delta;
        }
    }
}

fn owner_label(
    view: &Lookup<'_>,
    tenant: TenantId,
    kind: OwnerKind,
    owner: u64,
) -> Result<Option<String>, StorageError> {
    match kind {
        OwnerKind::Record => Ok(match view(&k::record_hdr_key(tenant, owner))? {
            Some(raw) => Some(
                k::decode_record_hdr(&raw)
                    .ok_or(StorageError::CorruptLog)?
                    .1
                    .to_owned(),
            ),
            None => None,
        }),
        OwnerKind::Relationship => {
            let Some(id) = view(&keys_ext::rel_by_ord_key(tenant, owner))? else {
                return Ok(None);
            };
            let id: [u8; 16] = id.try_into().map_err(|_| StorageError::CorruptLog)?;
            let Some(row) = view(&keys_ext::rel_row_key(tenant, &id))? else {
                return Ok(None);
            };
            let header: RelationshipHeader =
                serde_json::from_slice(&row).map_err(|_| StorageError::CorruptLog)?;
            Ok(Some(header.rel_type))
        }
    }
}

/// `(type, source label, target label)` of a relationship owner, from its
/// creation-time rel-group row.
type RelShape = (String, String, String);

fn rel_shape(
    view: &Lookup<'_>,
    tenant: TenantId,
    owner: u64,
) -> Result<Option<RelShape>, StorageError> {
    let Some(id) = view(&keys_ext::rel_by_ord_key(tenant, owner))? else {
        return Ok(None);
    };
    let id: [u8; 16] = id.try_into().map_err(|_| StorageError::CorruptLog)?;
    let group_key = keys_ext::rel_group_key(tenant, &id);
    let Some(raw) = view(&group_key)? else {
        return Ok(None);
    };
    rel_group(view, tenant, &group_key, &raw)
}

/// Values of one owner's pid: `(presence, entries, nulls, arrays)`.
struct PidState {
    presence: Option<Presence>,
    entries: Vec<Vec<u8>>,
    nulls: i64,
    array: bool,
}

fn pid_state(rows: &[(Vec<u8>, Vec<u8>)]) -> Result<PidState, StorageError> {
    let mut state = PidState {
        presence: None,
        entries: Vec::new(),
        nulls: 0,
        array: false,
    };
    for (_, raw) in rows {
        let occ = decode_occ_value(raw).map_err(|_| StorageError::CorruptLog)?;
        state.array |= occ.array;
        match occ.value {
            OccurrenceValue::Stored { .. } => {
                state.presence = Some(Presence::Value);
                state.entries.push(occ.entry.to_vec());
            }
            OccurrenceValue::ExplicitNull => {
                state.nulls += 1;
                state.presence.get_or_insert(Presence::Null);
            }
            OccurrenceValue::EmptyArray => {
                state.presence.get_or_insert(Presence::Null);
            }
        }
    }
    Ok(state)
}

/// Relationship type and creation-time endpoint labels of a rel-group row.
fn rel_group(
    view: &Lookup<'_>,
    tenant: TenantId,
    group_key: &[u8],
    group_raw: &[u8],
) -> Result<Option<(String, String, String)>, StorageError> {
    let (source, target) = serde_json::from_slice::<(String, String)>(group_raw)
        .map_err(|_| StorageError::CorruptLog)?;
    let id: [u8; 16] = group_key
        .get(group_key.len() - 16..)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(StorageError::CorruptLog)?;
    let Some(row) = view(&keys_ext::rel_row_key(tenant, &id))? else {
        return Ok(None);
    };
    let header: RelationshipHeader =
        serde_json::from_slice(&row).map_err(|_| StorageError::CorruptLog)?;
    Ok(Some((header.rel_type, source, target)))
}

/// `(node, outgoing)` of an adjacency key `…/adj/<out|in>/<node8>/…`.
fn parse_adj(key: &[u8]) -> Option<(u64, bool)> {
    let rest = key.get(TENANT_PREFIX_LEN..)?.strip_prefix(b"/adj/")?;
    let (outgoing, rest) = if let Some(rest) = rest.strip_prefix(b"out/") {
        (true, rest)
    } else {
        (false, rest.strip_prefix(b"in/")?)
    };
    Some((
        u64::from_be_bytes(rest.get(..8)?.try_into().ok()?),
        outgoing,
    ))
}

fn is_rel_group(key: &[u8]) -> bool {
    key.get(TENANT_PREFIX_LEN..)
        .is_some_and(|rest| rest.starts_with(b"/rel-group/"))
}

/// Owner-pid pairs a commit touched, and records whose header changed.
#[derive(Default)]
struct Touched {
    pids: BTreeSet<(TenantId, OwnerKind, u64, u64)>,
    relabeled: BTreeSet<(TenantId, u64)>,
    index: crate::index::IndexWork,
}

/// Fold one commit's base ops into label, degree and relationship-catalog
/// counters, and collect the owner pids whose occurrence-derived rows must be
/// recomputed.
fn scan_base_ops(
    after: &After<'_>,
    before: &dyn DeriveRead,
    changes: &mut Changes,
) -> Result<Touched, StorageError> {
    let read_before = |key: &[u8]| before.get(key);
    let read_after = |key: &[u8]| after.get(key);
    let mut touched = Touched::default();
    for key in after.ops.keys() {
        let Some(tenant) = k::parse_tenant(key) else {
            continue;
        };
        match key.get(TENANT_PREFIX_LEN) {
            Some(&k::F_OCC) => {
                let (_, kind, owner, pid, _) =
                    k::parse_occ_key(key).ok_or(StorageError::CorruptLog)?;
                touched.pids.insert((tenant, kind, owner, pid));
                if kind == OwnerKind::Record {
                    touched.index.occurrences.insert((tenant, owner, pid));
                }
            }
            Some(&k::F_RECORD_HDR) => {
                let owner = k::parse_record_hdr_key(key).ok_or(StorageError::CorruptLog)?;
                touched.relabeled.insert((tenant, owner));
                touched.index.headers.insert((tenant, owner));
            }
            Some(&k::F_INDEX_DEF) => {
                let id = key
                    .get(TENANT_PREFIX_LEN + 1..)
                    .and_then(|id| <[u8; 4]>::try_from(id).ok())
                    .map(u32::from_be_bytes)
                    .ok_or(StorageError::CorruptLog)?;
                touched.index.definitions.insert((tenant, id));
            }
            Some(&k::F_LABEL) => {
                let (label, _) = k::parse_label_member_key(key).ok_or(StorageError::CorruptLog)?;
                let was = before.get(key)?.is_some();
                let is = after.get(key)?.is_some();
                changes.bump(
                    k::label_count_key(tenant, label),
                    i64::from(is) - i64::from(was),
                );
            }
            _ if parse_adj(key).is_some() => {
                let (node, outgoing) = parse_adj(key).ok_or(StorageError::CorruptLog)?;
                let was = before.get(key)?.is_some();
                let is = after.get(key)?.is_some();
                changes.bump(
                    k::degree_key(tenant, node, outgoing),
                    i64::from(is) - i64::from(was),
                );
            }
            _ if is_rel_group(key) => {
                if let Some(raw) = before.get(key)? {
                    if let Some((rel_type, source, target)) =
                        rel_group(&read_before, tenant, key, &raw)?
                    {
                        changes.bump(k::rel_catalog_key(tenant, &rel_type, &source, &target), -1);
                    }
                }
                if let Some(raw) = after.get(key)? {
                    if let Some((rel_type, source, target)) =
                        rel_group(&read_after, tenant, key, &raw)?
                    {
                        changes.bump(k::rel_catalog_key(tenant, &rel_type, &source, &target), 1);
                    }
                }
            }
            _ => {}
        }
    }
    // A record whose header changed (create, delete, retype) moves every pid
    // it carries between labels.
    for (tenant, owner) in &touched.relabeled {
        let prefix = k::occ_owner_prefix(*tenant, OwnerKind::Record, *owner);
        for (key, _) in before
            .range_prefix(&prefix)?
            .into_iter()
            .chain(after.range_prefix(&prefix)?)
        {
            let (_, kind, owner, pid, _) =
                k::parse_occ_key(&key).ok_or(StorageError::CorruptLog)?;
            touched.pids.insert((*tenant, kind, owner, pid));
        }
    }
    Ok(touched)
}

/// `(label before, label after)` of one owner across a commit.
type OwnerLabels = (Option<String>, Option<String>);

/// One owner's occurrence rows grouped by pid, `(before, after)` a commit.
type PidRows = BTreeMap<u64, Vec<(Vec<u8>, Vec<u8>)>>;
type OwnerOccurrences = (PidRows, PidRows);

/// Group one owner's occurrence rows (key order) by pid.
fn rows_by_pid(rows: Vec<(Vec<u8>, Vec<u8>)>) -> Result<PidRows, StorageError> {
    let mut grouped = PidRows::new();
    for (key, value) in rows {
        let (_, _, _, pid, _) = k::parse_occ_key(&key).ok_or(StorageError::CorruptLog)?;
        grouped.entry(pid).or_default().push((key, value));
    }
    Ok(grouped)
}

impl Deriver for LmpgDeriver {
    fn derive(
        &self,
        _ci: u64,
        ops: &[Op],
        before: &dyn DeriveRead,
    ) -> Result<Vec<Op>, StorageError> {
        // Purge is a destructive tenant wipe that also erases the derived rows
        // themselves; it derives nothing.
        if ops.iter().any(|op| matches!(op, Op::Purge(_))) {
            return Ok(Vec::new());
        }
        let after = After::new(ops, before);
        let read_before = |key: &[u8]| before.get(key);
        let read_after = |key: &[u8]| after.get(key);
        let mut changes = Changes::default();
        let touched = scan_base_ops(&after, before, &mut changes)?;
        let mut labels: BTreeMap<(TenantId, OwnerKind, u64), OwnerLabels> = BTreeMap::new();
        let mut shapes: BTreeMap<(TenantId, u64), (Option<RelShape>, Option<RelShape>)> =
            BTreeMap::new();
        // One owner-wide occurrence scan per view, split by pid in memory:
        // `touched.pids` is ordered by owner, and a create touches every pid
        // of its owner, so this replaces a before and an after scan per pid.
        let mut owner_rows: Option<((TenantId, OwnerKind, u64), OwnerOccurrences)> = None;
        for &(tenant, kind, owner, pid) in &touched.pids {
            let (label_before, label_after) =
                if let Some(labels) = labels.get(&(tenant, kind, owner)) {
                    labels.clone()
                } else {
                    let resolved = (
                        owner_label(&read_before, tenant, kind, owner)?,
                        owner_label(&read_after, tenant, kind, owner)?,
                    );
                    labels.insert((tenant, kind, owner), resolved.clone());
                    resolved
                };
            if owner_rows
                .as_ref()
                .is_none_or(|(at, _)| *at != (tenant, kind, owner))
            {
                let prefix = k::occ_owner_prefix(tenant, kind, owner);
                owner_rows = Some((
                    (tenant, kind, owner),
                    (
                        rows_by_pid(before.range_prefix(&prefix)?)?,
                        rows_by_pid(after.range_prefix(&prefix)?)?,
                    ),
                ));
            }
            let Some((_, (rows_before, rows_after))) = owner_rows.as_ref() else {
                unreachable!("owner rows were just loaded")
            };
            let none = Vec::new();
            let old = pid_state(rows_before.get(&pid).unwrap_or(&none))?;
            let new = pid_state(rows_after.get(&pid).unwrap_or(&none))?;
            derive_pid(
                &mut changes,
                tenant,
                kind,
                owner,
                pid,
                (label_before.as_deref(), &old),
                (label_after.as_deref(), &new),
            );
            if kind == OwnerKind::Relationship {
                let (shape_before, shape_after) = match shapes.entry((tenant, owner)) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert((
                        rel_shape(&read_before, tenant, owner)?,
                        rel_shape(&read_after, tenant, owner)?,
                    )),
                };
                derive_shape_support(
                    &mut changes,

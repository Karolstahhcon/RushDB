//! Cross-store invariant scrubber over storage format v3 (DC11/DC12).
//!
//! The derived rows of a tenant (presence, value dictionary, counters, array
//! markers) must equal what [`LmpgDeriver`] derives **from scratch** from the
//! tenant's base rows — the same golden invariant the layout tests assert at
//! every commit. Base rows are cross-checked too: every occurrence has a live
//! owner and a known property, label membership matches record headers, the
//! public-id index matches its header, and adjacency is symmetric. An empty
//! result doubles as an invariant proof after any workload.

use std::collections::{BTreeMap, BTreeSet};

use rushdb_storage::{DeriveRead, Deriver, Op, StorageError};
use rushdb_types::ids::{OwnerKind, TenantId};
use rushdb_types::keys::{self as k, TENANT_PREFIX_LEN};
use rushdb_types::occurrence::decode_occ_value;

use crate::derived::LmpgDeriver;
use crate::reader::{keys_ext as kx, try_access, TryRead};

/// One invariant violation found by [`scrub`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// Machine-readable violation category.
    pub what: String,
    /// Human-readable context for the violation.
    pub detail: String,
}

/// Coverage and completion metadata for one scrub pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrubReport {
    /// Invariant violations found during the pass.
    pub violations: Vec<Violation>,
    /// Number of occurrence rows inspected.
    pub occurrence_rows: usize,
    /// Number of record header rows inspected.
    pub record_rows: usize,
    /// Number of derived rows compared against the from-scratch derivation.
    pub derived_rows: usize,
    /// True only after every requested range and point read completed.
    pub complete: bool,
}

/// Read-only candidate list for a bounded orphan repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanRepairPlan {
    /// Orphaned occurrence keys that a repair commit would remove.
    pub keys: Vec<Vec<u8>>,
    /// Whether the bounded scan reached the end of the occurrence range.
    pub complete: bool,
}

/// The empty prior state a from-scratch derivation starts from.
struct Nothing;

impl DeriveRead for Nothing {
    fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(None)
    }

    fn range_prefix(&self, _prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        Ok(Vec::new())
    }

    fn range(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        Ok(Vec::new())
    }
}

/// Full-scan consistency check of one tenant at the attached read view.
///
/// # Errors
///
/// Returns the underlying storage error when a range or point read cannot be
/// completed. A corrupt row that can be read is reported as a violation.
pub fn scrub<R: TryRead>(r: &R, t: TenantId) -> Result<Vec<Violation>, StorageError> {
    Ok(scrub_report(r, t)?.violations)
}

/// Derived rows a tenant's base rows imply, derived from scratch.
///
/// # Errors
/// Returns [`StorageError`] if the scan fails or a base row is corrupt.
pub fn derive_from_scratch<R: TryRead>(
    r: &R,
    t: TenantId,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, StorageError> {
    let deriver = LmpgDeriver;
    let base: Vec<Op> = r
        .range_prefix(&k::tenant_prefix(t))?
        .into_iter()
        .filter(|(key, _)| !deriver.is_derived_key(key))
        .map(|(key, value)| Op::Put(key, value))
        .collect();
    let mut out = BTreeMap::new();
    for op in deriver.derive(0, &base, &Nothing)? {
        match op {
            Op::Put(key, value) | Op::Set(key, value) => {
                out.insert(key, value);
            }
            Op::Del(key) | Op::Purge(key) => {
                out.remove(&key);
            }
        }
    }
    Ok(out)
}

/// Run a full scrub and return both violations and explicit coverage.
///
/// # Errors
///
/// Returns the underlying storage error when any scan or point read fails.
pub fn scrub_report<R: TryRead>(r: &R, t: TenantId) -> Result<ScrubReport, StorageError> {
    let rows = r.range_prefix(&k::tenant_prefix(t))?;
    let (mut violations, derived_rows) = derived_violations(r, t, &rows)?;
    let mut flag = |what: &str, detail: String| {
        violations.push(Violation {
            what: what.into(),
            detail,
        });
    };
    let mut occurrence_rows = 0usize;
    let mut record_rows = 0usize;
    for (key, value) in &rows {
        match key.get(TENANT_PREFIX_LEN) {
            Some(&k::F_OCC) => {
                occurrence_rows += 1;
                let Some((_, kind, owner, pid, _)) = k::parse_occ_key(key) else {
                    flag("occurrence_key_malformed", format!("{key:?}"));
                    continue;
                };
                if decode_occ_value(value).is_err() {
                    flag("occurrence_value_malformed", format!("{key:?}"));
                }
                if !owner_exists(r, t, kind, owner)? {
                    flag("occurrence_orphaned", format!("{kind:?}/{owner}/{pid}"));
                }
                if try_access::definition(r, t, pid)?.is_none() {
                    flag(
                        "occurrence_unknown_property",
                        format!("{kind:?}/{owner}/{pid}"),
                    );
                }
            }
            Some(&k::F_RECORD_HDR) => {
                record_rows += 1;
                let ordinal = k::parse_record_hdr_key(key).unwrap_or(u64::MAX);
                let Some((id, label)) = k::decode_record_hdr(value) else {
                    flag("record_header_malformed", format!("{ordinal}"));
                    continue;
                };
                if r.get(&k::label_member_key(t, label, ordinal))?.is_none() {
                    flag("label_membership_missing", format!("{label}/{ordinal}"));
                }
                let indexed = r.get(&k::record_id_key(t, &id))?;
                if indexed.as_deref() != Some(&ordinal.to_be_bytes()[..]) {
                    flag("record_id_index_mismatch", format!("{ordinal}"));
                }
            }
            Some(&k::F_RECORD_ID) => {
                // Each identity row must point at a header carrying that id;
                // a surplus row would resolve a second id to one record.
                let id: Option<[u8; 16]> = key
                    .get(TENANT_PREFIX_LEN + 1..)
                    .and_then(|id| id.try_into().ok());
                let ordinal = <[u8; 8]>::try_from(value.as_slice())
                    .ok()
                    .map(u64::from_be_bytes);
                let consistent = match (id, ordinal) {
                    (Some(id), Some(ordinal)) => try_access::record_by_ord(r, t, ordinal)?
                        .is_some_and(|header| header.record_id == id),
                    _ => false,
                };
                if !consistent {
                    flag("record_id_index_orphaned", k::describe_key(key));
                }
            }
            Some(&k::F_LABEL) => {
                let Some((label, ordinal)) = k::parse_label_member_key(key) else {
                    flag("label_membership_malformed", format!("{key:?}"));
                    continue;
                };
                match try_access::record_by_ord(r, t, ordinal)? {
                    Some(header) if header.label == label => {}
                    _ => flag("label_membership_stale", format!("{label}/{ordinal}")),
                }
            }
            _ => {}
        }
    }
    for violation in adjacency_violations(r, t)? {
        violations.push(violation);
    }
    Ok(ScrubReport {
        violations,

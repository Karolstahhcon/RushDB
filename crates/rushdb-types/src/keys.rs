use crate::ids::{OwnerKind, TenantId};
use std::fmt::Write as _;

/// Hex encoding used only for the small set of legacy-style, human-readable
/// tenant metadata keys. Canonical record, vector, text, and security state
/// remains under the binary tenant keyspace (`[TENANT_NS][tenant u32]/...`).
#[must_use]
pub fn tenant_hex(tenant: TenantId) -> String {
    let mut out = String::with_capacity(8);
    for byte in tenant.to_be_bytes() {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Namespace byte of system rows (not tenant-scoped), storage format v3.
pub const SYSTEM_NS: u8 = 0x00;
/// Namespace byte of tenant-owned rows, storage format v3.
pub const TENANT_NS: u8 = 0x01;
/// Length of the tenant key prefix `[TENANT_NS][tenant u32 BE]`.
pub const TENANT_PREFIX_LEN: usize = 5;

/// System family: tenant scope digest → `(ordinal u32 BE, state u8)`.
pub const SYS_TENANT_BY_SCOPE: u8 = 0x02;
/// System family: tenant ordinal → `(scope digest, state u8)`.
pub const SYS_TENANT_BY_ORD: u8 = 0x03;
/// System family: next tenant ordinal to assign (`u32 BE`).
pub const SYS_TENANT_SEQ: u8 = 0x04;

/// The 5-byte key prefix owned by `tenant`.
#[must_use]
pub fn tenant_prefix(tenant: TenantId) -> [u8; TENANT_PREFIX_LEN] {
    let [a, b, c, d] = tenant.to_be_bytes();
    [TENANT_NS, a, b, c, d]
}

/// The tenant owning a tenant-namespace key, if `key` is one.
#[must_use]
pub fn parse_tenant(key: &[u8]) -> Option<TenantId> {
    if key.first() != Some(&TENANT_NS) {
        return None;
    }
    let bytes: [u8; 4] = key.get(1..TENANT_PREFIX_LEN)?.try_into().ok()?;
    Some(TenantId(u32::from_be_bytes(bytes)))
}

/// Directory row: scope digest → ordinal.
#[must_use]
pub fn tenant_by_scope_key(scope: &crate::ids::TenantScope) -> Vec<u8> {
    let mut out = Vec::with_capacity(34);
    out.extend_from_slice(&[SYSTEM_NS, SYS_TENANT_BY_SCOPE]);
    out.extend_from_slice(&scope.0);
    out
}

/// Directory row: ordinal → scope digest.
#[must_use]
pub fn tenant_by_ord_key(tenant: TenantId) -> Vec<u8> {
    let mut out = Vec::with_capacity(6);
    out.extend_from_slice(&[SYSTEM_NS, SYS_TENANT_BY_ORD]);
    out.extend_from_slice(&tenant.to_be_bytes());
    out
}

/// Prefix of every ordinal → scope directory row (tenant enumeration).
#[must_use]
pub fn tenant_by_ord_prefix() -> [u8; 2] {
    [SYSTEM_NS, SYS_TENANT_BY_ORD]
}

/// Directory row holding the next ordinal to assign.
#[must_use]
pub fn tenant_seq_key() -> [u8; 2] {
    [SYSTEM_NS, SYS_TENANT_SEQ]
}

/// Prefix for durable vector-index definitions owned by one tenant.
#[must_use]
pub fn vector_index_meta_prefix(tenant: TenantId) -> String {
    format!("vector/index/{}/", tenant_hex(tenant))
}

/// Prefix for durable ANN manifests owned by one tenant.
#[must_use]
pub fn ann_manifest_meta_prefix(tenant: TenantId) -> String {
    format!("vector/ann-manifest/{}/", tenant_hex(tenant))
}

/// Key segment for tenant metadata rows.
pub const P_META: &[u8] = b"meta";
/// Key segment for relationship rows by public id.
pub const P_REL: &[u8] = b"relationship";
/// Key segment for relationship rows by dense ordinal.
pub const P_REL_BY_ORD: &[u8] = b"relationship-ordinal";
/// Per-relationship captured endpoint labels (cascade-safe decrement source).
pub const P_REL_GROUP: &[u8] = b"rel-group";
/// Key segment for property-family signatures.
pub const P_FAMILY_SIG: &[u8] = b"property-family-signature";
/// Key segment for property-family rows.
pub const P_FAMILY: &[u8] = b"property-family";
/// Key segment for property-definition rows.
pub const P_DEFINITION: &[u8] = b"property-definition";
/// Build a tenant keyspace key: `[TENANT_NS][tenant u32 BE]` followed by
/// `/`-separated segments.
#[must_use]
pub fn key(tenant: TenantId, segments: &[&[u8]]) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(TENANT_PREFIX_LEN + segments.iter().map(|s| s.len() + 1).sum::<usize>());
    out.extend_from_slice(&tenant_prefix(tenant));
    for s in segments {
        out.push(b'/');
        out.extend_from_slice(s);
    }
    out
}

/// Inclusive start / exclusive end range covering all keys under a prefix.
///
/// # Examples
///
/// ```
/// use rushdb_types::keys::prefix_range;
///
/// let (start, end) = prefix_range(b"ab");
/// assert_eq!(start, b"ab");
/// assert_eq!(end, b"ac");
/// ```
#[must_use]
pub fn prefix_range(prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut end = prefix.to_vec();
    loop {
        match end.last_mut() {
            Some(last) if *last != 0xFF => {
                *last += 1;
                return (prefix.to_vec(), end);
            }
            Some(_) => {
                end.pop();
            }
            None => return (prefix.to_vec(), vec![]),
        }
    }
}

/// Big-endian 8-byte encoding of an ordinal, used as a key segment.
#[must_use]
pub fn u64be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

/// Key prefix for every row owned by records or relationships.
#[must_use]
pub fn owner_prefix(kind: OwnerKind) -> &'static [u8] {
    match kind {
        OwnerKind::Record => b"record",
        OwnerKind::Relationship => b"rel",
    }
}

/// Key segment for embedding-space definitions.
pub const P_EMB_SPACE: &[u8] = b"emb-space";
/// Key segment for embedding-space alias mappings.
pub const P_EMB_ALIAS: &[u8] = b"emb-alias";
/// Key segment for embedding bindings.
pub const P_EMB_BINDING: &[u8] = b"emb-binding";
/// Key segment for stored embedding vectors.
pub const P_EMB_VECTOR: &[u8] = b"vec";

/// Key for an embedding-space definition by space id.
#[must_use]
pub fn emb_space_key(tenant: TenantId, sid: u64) -> Vec<u8> {
    key(tenant, &[P_EMB_SPACE, &u64be(sid)])
}

/// Key mapping an embedding-space alias to its space.
#[must_use]
pub fn emb_alias_key(tenant: TenantId, alias: &str) -> Vec<u8> {
    key(tenant, &[P_EMB_ALIAS, alias.as_bytes()])
}

/// Key for one embedding binding of an owner property element.
#[must_use]
pub fn emb_binding_key(
    tenant: TenantId,
    space: u64,
    kind: OwnerKind,
    owner_ord: u64,
    pid: u64,
    elem: u64,
) -> Vec<u8> {
    key(
        tenant,
        &[
            P_EMB_BINDING,
            &u64be(space),
            &[kind.as_byte()],
            &u64be(owner_ord),
            &u64be(pid),
            &u64be(elem),
        ],
    )
}

/// Prefix covering every embedding binding in a space.
#[must_use]
pub fn emb_bindings_prefix(tenant: TenantId, space: u64) -> Vec<u8> {
    key(tenant, &[P_EMB_BINDING, &u64be(space)])
}

/// Key for a stored embedding vector slot within a space.
#[must_use]
pub fn emb_vector_key(tenant: TenantId, space: u64, slot: u64) -> Vec<u8> {
    key(tenant, &[P_EMB_VECTOR, &u64be(space), &u64be(slot)])
}

/// Key segment for per-space binding versions.
pub const P_EMB_VERSION: &[u8] = b"emb-version";

/// Version row of a space's binding set: `prev_ci || ci` (16 bytes), the
/// commit that last changed the space's bindings and the one before it. A
/// cached ANN generation is current when its version equals `ci`, and an
/// incremental absorb of commit `ci` chains onto it when its version equals
/// `prev_ci` — independent of any other space or tenant's commits.
#[must_use]
pub fn emb_space_version_key(tenant: TenantId, space: u64) -> Vec<u8> {
    key(tenant, &[P_EMB_VERSION, &u64be(space)])
}

/// Encode a space version row.
#[must_use]
pub fn emb_space_version_value(prev_ci: u64, ci: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&u64be(prev_ci));
    out.extend_from_slice(&u64be(ci));
    out
}

/// Decode a space version row; `None` for a malformed value.
#[must_use]
pub fn parse_emb_space_version(raw: &[u8]) -> Option<(u64, u64)> {
    let prev = u64::from_be_bytes(raw.get(..8)?.try_into().ok()?);
    let ci = u64::from_be_bytes(raw.get(8..16)?.try_into().ok()?);
    (raw.len() == 16).then_some((prev, ci))
}

/// Key segment for per-space ready-vector counts.
pub const P_EMB_READY: &[u8] = b"emb-ready";

/// Number of a space's bindings in `Ready` status, maintained by every
/// commit that writes a binding (big-endian `u64`). It is the pool a vector
/// search ranks, read in one `get` to choose exact or ANN search.
#[must_use]
pub fn emb_ready_count_key(tenant: TenantId, space: u64) -> Vec<u8> {
    key(tenant, &[P_EMB_READY, &u64be(space)])
}

/// Key segment for pending embedding-slot reclamations.
pub const P_EMB_RECLAIM: &[u8] = b"emb-reclaim";

/// Pending reclamation of a vector slot superseded at commit `after_ci`. The
/// slot's bytes may still be read by a pinned view until the GC watermark
/// passes `after_ci`; the reclamation sweep deletes them only then. Ordered by
/// `after_ci` so the sweep ranges straight to the reclaimable rows.
#[must_use]
pub fn emb_reclaim_key(tenant: TenantId, after_ci: u64, space: u64, slot: u64) -> Vec<u8> {
    key(
        tenant,
        &[P_EMB_RECLAIM, &u64be(after_ci), &u64be(space), &u64be(slot)],
    )
}

/// Prefix covering every pending embedding-slot reclamation for a tenant.
#[must_use]
pub fn emb_reclaim_prefix(tenant: TenantId) -> Vec<u8> {
    key(tenant, &[P_EMB_RECLAIM])
}

/// Value of a reclamation row: `after_ci || space || slot` (24 bytes), so the
/// sweep can recover the vector key to delete without re-parsing the key.
#[must_use]
pub fn emb_reclaim_value(after_ci: u64, space: u64, slot: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&u64be(after_ci));
    out.extend_from_slice(&u64be(space));
    out.extend_from_slice(&u64be(slot));
    out
}

/// Decode a reclamation row value into `(after_ci, space, slot)`.
#[must_use]
pub fn parse_emb_reclaim_value(value: &[u8]) -> Option<(u64, u64, u64)> {
    if value.len() != 24 {
        return None;
    }
    let after_ci = u64::from_be_bytes(value[0..8].try_into().ok()?);
    let space = u64::from_be_bytes(value[8..16].try_into().ok()?);
    let slot = u64::from_be_bytes(value[16..24].try_into().ok()?);
    Some((after_ci, space, slot))
}

/// Legacy raw global generation key. Retained only for old-command replay and
/// migration detection; new writes use `security_generation_key(tenant)`.
pub const SECURITY_GENERATION_KEY: &str = "security/generation";

/// Legacy raw global policy key; new writes use `security_policy_key(tenant)`.
pub const SECURITY_POLICY_ACTIVE_KEY: &str = "security/policy/active";

/// Prefix covering all security rows of a tenant.
#[must_use]
pub fn security_prefix(tenant: TenantId) -> Vec<u8> {
    key(tenant, &[b"security", b""])
}

/// Key for a tenant's security generation watermark.
#[must_use]
pub fn security_generation_key(tenant: TenantId) -> Vec<u8> {
    key(tenant, &[b"security", b"generation"])
}

/// Key for a tenant's active security policy.
#[must_use]
pub fn security_policy_key(tenant: TenantId) -> Vec<u8> {
    key(tenant, &[b"security", b"policy", b"active"])
}

// ---- storage format v3 typed families -------------------------
//
// Typed keys are `[TENANT_NS][tenant u32 BE][family u8][fields…]`. Family tags
// are below `/` (0x2F), so they never collide with the `/`-separated generic
// keys built by [`key`]. Integers are fixed-width big-endian; a variable-length
// string that is not the last field is `u16 BE length + bytes`.

/// Typed family: public record id → record ordinal.
pub const F_RECORD_ID: u8 = 0x03;
/// Typed family: record ordinal → `(public id, label)` header.
pub const F_RECORD_HDR: u8 = 0x04;
/// Typed family: `(label, record ordinal)` label membership.
pub const F_LABEL: u8 = 0x05;
/// Typed family: derived counters (see the `C_*` sub-families).
pub const F_COUNTER: u8 = 0x09;
/// Typed family: per-`(kind, label, pid)` value dictionary with refcounts.
pub const F_VALUE_DICT: u8 = 0x0A;
/// Typed family: sticky array markers.
pub const F_MARKER: u8 = 0x11;
/// Typed family: one canonical occurrence per `(kind, owner, pid, elem)`.
pub const F_OCC: u8 = 0x12;
/// Typed family: `(kind, label, pid, nullness, owner)` presence.
pub const F_PRESENCE: u8 = 0x13;
/// Typed family: index id → [`crate::index::IndexDef`] (JSON).
pub const F_INDEX_DEF: u8 = 0x14;
/// Typed family: index name → index id.
pub const F_INDEX_NAME: u8 = 0x15;
/// Typed family (derived): `(index id, tag, payload, owner)` ordered entry.
pub const F_INDEX_ENTRY: u8 = 0x16;
/// Typed family: `(text index id, owner)` → commit index of the newest
/// change the text applier has not yet indexed (base rows).
pub const F_INDEX_PENDING: u8 = 0x17;
/// Typed family: the tenant's logical stored bytes (key + value lengths of
/// every live row under the tenant prefix except this one), maintained by the
/// storage backend in the batch that changes them.
pub const F_USAGE_BYTES: u8 = 0x18;

/// Coarse family name of any store key, for accounting and diagnostics:
/// tenant families by their family byte, legacy segment keys by their first
/// segment, and system/global keys by namespace. One classifier serves the
/// layout and read-accounting tools.
#[must_use]
pub fn family_label(key: &[u8]) -> String {
    match key.first() {
        Some(&TENANT_NS) if key.len() > TENANT_PREFIX_LEN => {
            let rest = &key[TENANT_PREFIX_LEN..];
            match rest[0] {
                F_RECORD_ID => "record-id".into(),
                F_RECORD_HDR => "record-header".into(),
                F_LABEL => "label-member".into(),
                F_COUNTER => format!("counter/{:02x}", rest.get(1).copied().unwrap_or(0)),
                F_VALUE_DICT => "value-dictionary".into(),
                F_MARKER => "marker".into(),
                F_OCC => "occurrence".into(),
                F_PRESENCE => "presence".into(),
                F_INDEX_DEF => "index-def".into(),
                F_INDEX_NAME => "index-name".into(),
                F_INDEX_ENTRY => "index-entry".into(),
                F_INDEX_PENDING => "index-pending".into(),
                F_USAGE_BYTES => "usage-bytes".into(),
                b'/' => {
                    let segment = rest[1..].split(|byte| *byte == b'/').next().unwrap_or(&[]);

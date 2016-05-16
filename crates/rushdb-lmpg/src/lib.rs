//! Deterministic expansion of committed commands into the Labeled
//! Meta-Property Graph (LMPG) layout, plus the readers over that state.
//!
//! [`expand::expand_commit`] is pure with respect to `(state, batch)` and runs
//! on both the forward publish path and crash-recovery replay; it materializes
//! occurrences, equality/range/presence postings, the label and relationship
//! catalogs, and owner-grain support counters. The [`reader`] module exposes
//! the corresponding lookups so the query planner never re-derives them.
/// Persistent, versioned derived aggregates (the "store facts, derive
/// aggregates" model); see the module docs for the invariant.
pub mod derived;

/// Deterministic materialization of committed batches into occurrence,
/// posting, catalog and counter keys, plus tenant-purge key enumeration.
pub mod expand;

/// Keys erased per tenant-purge commit. Bounds both the materialized key set
/// and the write transaction so purge scales to arbitrarily large tenants;
/// the driver loops until the tenant is empty.
const PURGE_CHUNK_KEYS: usize = 1024;

/// NDJSON/CSV/JSON importers that coerce external documents into LMPG
/// records, relationships and vector drafts.
pub mod import;
/// Declared ordered indexes.
pub mod index;
mod limits;
pub use limits::{QueryLimits, RequestLimits};

/// Mutation planning: field resolution, type-state transitions, occurrence
/// staging and precondition collection over a transaction overlay.
pub mod plan;

/// Read-only accessors over committed LMPG state.
pub mod reader;

/// Integrity checks that report LMPG invariant violations in a snapshot.
pub mod scrub;
/// Tenant directory resolution and registration.
pub mod tenancy;

/// Fallible rebuilds that surface storage errors instead of panicking.
pub mod reconstruct;

/// Type-enforced `DataAccessPolicy` gate over record/relationship
/// reconstruction (tenancy hardening, Pillar 4).
pub mod authorized_view;

pub use authorized_view::AuthorizedView;

/// The data-access policy as catalog reads apply it.
pub mod catalog_access;

pub use catalog_access::CatalogAccess;
pub use plan::{FieldChange, PlanError, ValuePayload};

use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use expand::expand_commit;
use plan::Planner;
use reader::keys_ext as kx;
use rushdb_storage::{CommitOutcome, Store};
use rushdb_types::command::{CommitBatch, IsolationMode, Mutation, Precondition};
use rushdb_types::ids::OwnerKind;
use rushdb_types::occurrence::RelationshipHeader;
/// Errors surfaced by [`Lmpg`] mutation entry points.
#[derive(Debug, thiserror::Error)]
pub enum LmpgError {
    /// Mutation planning failed (type conflict, missing entity, storage error).
    #[error("planning failed: {0}")]
    Plan(#[from] PlanError),
    /// The commit lost an optimistic race or was rejected by policy.
    #[error("{0}")]
    Conflict(String),
    /// A referenced record or relationship no longer exists.
    #[error("entity not found: {0}")]
    NotFound(String),
    /// A batch planned at a snapshot lost to a later commit in the same
    /// tenant (first-committer-wins); retrying re-plans it on fresh data.
    #[error("write_conflict: {0}")]
    WriteConflict(String),
}

impl LmpgError {
    /// Renders the error as the short human-readable reason stored in commit
    /// outcomes and returned across the transaction boundary.
    #[must_use]
    pub fn to_reason(&self) -> String {
        match self {
            LmpgError::Plan(p) => format!("plan: {p}"),
            LmpgError::Conflict(m) => m.clone(),
            LmpgError::NotFound(m) => format!("not found: {m}"),
            LmpgError::WriteConflict(m) => format!("write_conflict: {m}"),
        }
    }
}

/// Stable public handle for a record: its 16-byte UUID plus allocated ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecordRef {
    /// Stable public record UUID.
    pub record_id: [u8; 16],
    /// Monotonic storage ordinal used for owner-grain keys.
    pub ordinal: u64,
}

/// Stable public handle for a relationship: its 16-byte UUID plus ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RelationshipRef {
    /// Stable public relationship UUID.
    pub relationship_id: [u8; 16],
    /// Monotonic storage ordinal used for owner-grain keys.
    pub ordinal: u64,
}

/// Convenience alias for results produced by [`Lmpg`] operations.
pub type LmpgResult<T> = Result<T, LmpgError>;

/// Fine-grained authorization hook evaluated inside the engine on every
/// mutation path (document 23): default-deny label/relationship checks and
/// commit-time policy-generation revalidation. An opaque transaction id is
/// never authority — the hook is re-evaluated against live policy state.
pub trait Authorizer: Send + Sync {
    /// Authorizes writing a record under `label` against live policy state.
    ///
    /// # Errors
    /// Returns a policy reason when the label is not permitted.
    fn authorize_label(&self, label: &str) -> Result<(), String>;
    /// Authorizes writing a relationship of `rel_type` against live policy.
    ///
    /// # Errors
    /// Returns a policy reason when the relationship type is not permitted.
    fn authorize_rel_type(&self, rel_type: &str) -> Result<(), String>;
    /// Current committed policy generation; commit revalidates against this.
    ///
    /// # Errors
    /// Returns a policy reason when the generation cannot be read.
    fn current_generation(&self) -> Result<u64, String>;
    /// Admitted checks must use this transaction, not acquire another store
    /// view while writer/map locks are held. `cap` is the admitted predecessor.
    ///
    /// # Errors
    /// Returns a policy reason when the label is not permitted in `txn`.
    fn authorize_label_in_txn(
        &self,
        label: &str,
        txn: &rushdb_storage::Txn<'_>,
        cap: u64,
    ) -> Result<(), String>;
    /// Transactional counterpart of [`Authorizer::authorize_rel_type`].
    ///
    /// # Errors
    /// Returns a policy reason when the relationship type is not permitted
    /// in `txn`.
    fn authorize_rel_type_in_txn(
        &self,
        rel_type: &str,
        txn: &rushdb_storage::Txn<'_>,
        cap: u64,
    ) -> Result<(), String>;
    /// Transactional counterpart of [`Authorizer::current_generation`].
    ///
    /// # Errors
    /// Returns a policy reason when the generation cannot be read in `txn`.
    fn current_generation_in_txn(
        &self,
        txn: &rushdb_storage::Txn<'_>,
        cap: u64,
    ) -> Result<u64, String>;
}

/// Open a durable store for LMPG with the apply-time deriver installed
/// **before** WAL recovery, so recovered commits derive the same presence,
/// dictionary and counter rows live publication would have.
///
/// # Errors
/// Returns [`rushdb_storage::StorageError`] if the store cannot be opened or
/// recovery fails.
pub fn open_store(path: &std::path::Path) -> Result<Store, rushdb_storage::StorageError> {
    open_store_with_resources(path, rushdb_storage::resources::StorageResources::default())
}

/// [`open_store`] with an explicit resource configuration.
///
/// # Errors
/// Returns [`rushdb_storage::StorageError`] if `resources` is invalid, the
/// store cannot be opened, or recovery fails.
pub fn open_store_with_resources(
    path: &std::path::Path,
    resources: rushdb_storage::resources::StorageResources,
) -> Result<Store, rushdb_storage::StorageError> {
    Store::open_with_deriver(path, resources, Some(Arc::new(derived::LmpgDeriver)))
}

/// One tenant facade over one replicated KV substrate.
pub struct Lmpg {
    /// Tenant whose keys this facade reads and writes.
    pub tenant: rushdb_types::ids::TenantId,
    /// Shared storage handle owning the commit log and expander.
    pub store: Arc<Store>,
    authorizer: std::sync::RwLock<Option<Arc<dyn Authorizer>>>,
    limits: QueryLimits,
}

impl Lmpg {
    /// Wires the deterministic expander used on publish and replay paths.
    pub fn new(store: Arc<Store>, tenant: rushdb_types::ids::TenantId) -> Self {
        let cb: rushdb_storage::ExpandFn =
            Box::new(|ci, batch, txn| expand_commit(ci, batch, txn).map_err(|e| e.to_string()));
        store.set_expander(Arc::new(cb));
        // Derived rows (presence, value dictionary, counters, markers) are
        // written by the deriver at apply time. Durable stores must be
        // opened with it already installed (`open_store`), so WAL recovery
        // derives them too; installing it again here is idempotent.
        store.set_deriver(Arc::new(derived::LmpgDeriver));
        Lmpg::new_with_authorizer(store, tenant, None)
    }

    /// Builds a facade with an optional authorization hook, without
    /// installing the expander (call [`Lmpg::new`] for that).
    pub fn new_with_authorizer(
        store: Arc<Store>,
        tenant: rushdb_types::ids::TenantId,
        authorizer: Option<Arc<dyn Authorizer>>,
    ) -> Lmpg {
        let limits = QueryLimits::of_server(&store.resource_config());
        Lmpg {
            tenant,
            store,
            authorizer: std::sync::RwLock::new(authorizer),
            limits,
        }
    }

    /// This handle with the server's ceilings tightened by one request's
    /// own bounds.
    #[must_use]
    pub fn with_request_limits(mut self, request: RequestLimits) -> Self {
        self.limits = self.limits.tightened(request);
        self
    }

    /// The bounds this handle's reads run under.
    #[must_use]
    pub fn limits(&self) -> QueryLimits {
        self.limits
    }

    /// Another handle on the same tenant, authorizer and bounds.
    #[must_use]
    pub fn handle(&self) -> Lmpg {
        Lmpg {
            tenant: self.tenant,
            store: Arc::clone(&self.store),
            authorizer: std::sync::RwLock::new(
                self.authorizer
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
            ),
            limits: self.limits,
        }
    }

    /// Installs or replaces the authorization hook used by all mutations. A
    /// poisoned lock is recovered (the value is a single `Option` assignment,
    /// so poison cannot leave it corrupt).
    pub fn set_authorizer(&self, a: Arc<dyn Authorizer>) {
        *self
            .authorizer
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(a);
    }

    /// Default-deny label gate used by every record mutation path.
    pub(crate) fn authorize_label(&self, label: &str) -> Result<(), String> {
        if let Some(a) = self
            .authorizer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            a.authorize_label(label)?;
        }
        Ok(())
    }

    pub(crate) fn authorize_rel_type(&self, rel_type: &str) -> Result<(), String> {
        if let Some(a) = self
            .authorizer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            a.authorize_rel_type(rel_type)?;
        }
        Ok(())
    }

    fn authorize_label_in_txn(
        &self,
        label: &str,
        txn: &rushdb_storage::Txn<'_>,
        cap: u64,
    ) -> Result<(), String> {
        if let Some(authorizer) = self
            .authorizer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            authorizer.authorize_label_in_txn(label, txn, cap)?;
        }
        Ok(())
    }

    fn authorize_rel_type_in_txn(
        &self,
        rel_type: &str,
        txn: &rushdb_storage::Txn<'_>,
        cap: u64,
    ) -> Result<(), String> {
        if let Some(authorizer) = self
            .authorizer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            authorizer.authorize_rel_type_in_txn(rel_type, txn, cap)?;
        }
        Ok(())
    }

    /// Creates a record under `label`, optionally with an explicit public id,
    /// and returns its stable reference.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if planning fails, the label is denied, or the
    /// commit conflicts.
    pub fn create_record(
        &self,
        label: &str,
        explicit_id: Option<[u8; 16]>,
        fields: Vec<(String, FieldChange)>,
    ) -> LmpgResult<RecordRef> {
        self.create_record_tx(label, explicit_id, fields, None)
    }

    /// Cross-service transaction variant: plan the record against the
    /// transaction's overlay (read-your-writes) and append the writes to the
    /// transaction instead of committing.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if planning fails, the label is denied, or the
    /// commit conflicts.
    pub fn create_record_tx(
        &self,
        label: &str,
        explicit_id: Option<[u8; 16]>,
        fields: Vec<(String, FieldChange)>,
        tx: Option<&mut rushdb_storage::TxAccumulator>,
    ) -> LmpgResult<RecordRef> {
        self.authorize_label(label).map_err(LmpgError::Conflict)?;
        let rid = explicit_id.unwrap_or_else(|| *Uuid::now_v7().as_bytes());
        let tenant = self.tenant;
        let label_owned = label.to_string();

        let build = |ci: u64, txn: &mut rushdb_storage::Txn<'_>| {
            self.authorize_label_in_txn(label, txn, ci.saturating_sub(1))?;
            let mut pl = Planner::new(tenant, txn);
            pl.push_precondition(Precondition::absent(rushdb_types::keys::record_id_key(
                tenant, &rid,
            )));
            let ordinal = pl
                .next_owner(OwnerKind::Record)
                .map_err(|e| e.to_string())?;
            pl.mutations.push(Mutation::CreateRecordHeader {
                record_id: rid,
                ordinal,
                label: label_owned.clone(),
            });

            for (name, change) in fields {
                plan_field_write(&mut pl, OwnerKind::Record, ordinal, &name, &change)
                    .map_err(|e| e.to_string())?;
            }

            Ok((
                finish_batch(&mut pl, ci),
                RecordRef {
                    record_id: rid,
                    ordinal,
                },
            ))
        };

        match tx {
            Some(tx) => self
                .store
                .tx_apply(tx, Uuid::now_v7(), build)
                .map_err(|e| LmpgError::Conflict(e.to_string())),
            None => finalize(
                self.store
                    .commit_for_tenant(&self.tenant, Uuid::now_v7(), build),
            ),
        }
    }

    /// Ensure the Record-scoped Property Family for `display_name` exists
    /// (schema emergence). Used by inline-vector binding so an external
    /// embedding property can be indexed without a stored occurrence.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the family cannot be resolved or the commit
    /// conflicts.
    pub fn ensure_record_family(&self, display_name: &str) -> LmpgResult<u64> {
        self.ensure_family(OwnerKind::Record, display_name)
    }

    /// Ensure the Relationship-scoped Property Family for `display_name`
    /// exists. Used by relationship-owned vector indexes so an external
    /// embedding property can be bound to a relationship without a stored
    /// occurrence.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the family cannot be resolved or the commit
    /// conflicts.
    pub fn ensure_relationship_family(&self, display_name: &str) -> LmpgResult<u64> {
        self.ensure_family(OwnerKind::Relationship, display_name)
    }

    /// Ensure a Property Family exists for the given owner kind.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the family cannot be resolved or the commit
    /// conflicts.
    pub fn ensure_family(&self, owner_kind: OwnerKind, display_name: &str) -> LmpgResult<u64> {
        let tenant = self.tenant;
        // Fast path: an existing family needs no commit (and no fsync). Any
        // read failure falls through to the write path below, which
        // re-resolves under admission and reports the error itself.
        let signature = reader::keys_ext::family_sig_key(
            tenant,
            "default",
            owner_kind,
            &rushdb_types::value::normalize_name(display_name),
        );
        let existing = self
            .store
            .read_view()
            .ok()
            .and_then(|view| view.get(&signature).ok().flatten());
        if let Some(id) = existing
            .as_deref()
            .and_then(|raw| raw.get(..8))
            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        {
            return Ok(u64::from_be_bytes(id));
        }
        let dn = display_name.to_string();
        let outcome = self
            .store
            .commit_for_tenant(&self.tenant, Uuid::now_v7(), move |ci, txn| {
                let mut pl = Planner::new(tenant, txn);
                let (fam, _) = pl
                    .resolve_family("default", owner_kind, &dn)
                    .map_err(|e| e.to_string())?;
                Ok((finish_batch(&mut pl, ci), fam.family_id))
            });
        finalize(outcome)
    }

    /// Replaces the supplied fields on `record_ref` in one commit.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the record is missing, the label is denied, or
    /// the commit conflicts.
    pub fn update_fields(
        &self,
        record_ref: RecordRef,
        fields: Vec<(String, FieldChange)>,
    ) -> LmpgResult<()> {
        self.update_fields_tx(record_ref, fields, None)
    }

    /// Cross-service transaction variant of [`Self::update_fields`].
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the record is missing, the label is denied, or
    /// the commit conflicts.
    pub fn update_fields_tx(
        &self,
        record_ref: RecordRef,
        fields: Vec<(String, FieldChange)>,
        tx: Option<&mut rushdb_storage::TxAccumulator>,
    ) -> LmpgResult<()> {
        self.apply_owner_fields_tx(
            OwnerKind::Record,
            record_ref.ordinal,
            record_ref.record_id,
            fields,
            tx,
        )
    }

    pub(crate) fn apply_owner_fields_tx(
        &self,
        kind: OwnerKind,
        owner_ord: u64,
        owner_public_id: [u8; 16],
        fields: Vec<(String, FieldChange)>,
        tx: Option<&mut rushdb_storage::TxAccumulator>,
    ) -> LmpgResult<()> {
        debug_assert_eq!(kind, OwnerKind::Record);
        let tenant = self.tenant;
        run_mutation(self, tx, |ci, txn| {
            let hdr = crate::reader::try_access::record_by_id(
                txn,
                tenant,
                &rushdb_types::ids::RecordId(owner_public_id),
            )
            .map_err(|e| e.to_string())?
            .ok_or("record vanished")?;
            if hdr.ordinal != owner_ord {
                return Err("record identity mismatch".into());
            }
            self.authorize_label_in_txn(&hdr.label, txn, ci.saturating_sub(1))?;
            let mut pl = Planner::new(tenant, txn);
            pl.push_precondition(Precondition::exists(rushdb_types::keys::record_id_key(
                tenant,
                &owner_public_id,
            )));

            for (name, change) in fields {
                plan_field_write(&mut pl, kind, owner_ord, &name, &change)
                    .map_err(|e| e.to_string())?;
            }

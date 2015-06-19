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

            Ok((finish_batch(&mut pl, ci), ()))
        })
    }

    /// Deletes a record together with its occurrences and incident edges.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the record is missing, the label is denied, or
    /// the commit conflicts.
    pub fn delete_record(&self, record_ref: RecordRef) -> LmpgResult<()> {
        self.delete_record_tx(record_ref, None)
    }

    /// Atomically change a record's sole primary label without changing its
    /// public ID, ordinal, fields, vectors, or relationships. `from_label` is
    /// an optimistic precondition, preventing a stale caller from silently
    /// overwriting a concurrent reclassification.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if either label is empty, the labels are equal,
    /// a label is denied, the record is stale, or the commit conflicts.
    pub fn retype_record(
        &self,
        record_ref: RecordRef,
        from_label: &str,
        to_label: &str,
    ) -> LmpgResult<()> {
        self.retype_record_tx(record_ref, from_label, to_label, None)
    }

    /// Transaction variant of [`Self::retype_record`].
    ///
    /// # Errors
    /// Returns [`LmpgError`] if either label is empty, the labels are equal,
    /// a label is denied, the record is stale, or the commit conflicts.
    pub fn retype_record_tx(
        &self,
        record_ref: RecordRef,
        from_label: &str,
        to_label: &str,
        tx: Option<&mut rushdb_storage::TxAccumulator>,
    ) -> LmpgResult<()> {
        if from_label.trim().is_empty() || to_label.trim().is_empty() {
            return Err(LmpgError::Conflict(
                "record labels must be non-empty".into(),
            ));
        }
        if from_label == to_label {
            return Err(LmpgError::Conflict(
                "record already has the requested label".into(),
            ));
        }
        self.authorize_label(from_label)
            .map_err(LmpgError::Conflict)?;
        self.authorize_label(to_label)
            .map_err(LmpgError::Conflict)?;
        let tenant = self.tenant;
        let from_label = from_label.to_owned();
        let to_label = to_label.to_owned();
        run_mutation(self, tx, |ci, txn| {
            self.authorize_label_in_txn(&from_label, txn, ci.saturating_sub(1))?;
            self.authorize_label_in_txn(&to_label, txn, ci.saturating_sub(1))?;
            let header = record_header_in(txn, tenant, &record_ref.record_id)?
                .ok_or_else(|| "record vanished".to_string())?;
            if header.ordinal != record_ref.ordinal || header.label != from_label {
                return Err("record label changed or record reference is stale".into());
            }
            let mut pl = Planner::new(tenant, txn);
            pl.push_precondition(Precondition::exists(rushdb_types::keys::record_id_key(
                tenant,
                &record_ref.record_id,
            )));
            pl.mutations.push(Mutation::RetypeRecordHeader {
                record_id: record_ref.record_id,
                ordinal: record_ref.ordinal,
                from_label,
                to_label,
            });
            Ok((finish_batch(&mut pl, ci), ()))
        })
    }

    /// Cross-service transaction variant of [`Self::delete_record`].
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the record is missing, the label is denied, or
    /// the commit conflicts.
    pub fn delete_record_tx(
        &self,
        record_ref: RecordRef,
        tx: Option<&mut rushdb_storage::TxAccumulator>,
    ) -> LmpgResult<()> {
        let tenant = self.tenant;
        run_mutation(self, tx, |ci, txn| {
            let mut pl = Planner::new(tenant, txn);
            pl.push_precondition(Precondition::exists(rushdb_types::keys::record_id_key(
                tenant,
                &record_ref.record_id,
            )));

            let hdr = record_header_in(txn, tenant, &record_ref.record_id)?
                .ok_or_else(|| "record vanished".to_string())?;

            self.authorize_label_in_txn(&hdr.label, txn, ci.saturating_sub(1))?;

            cascade_record_delete(&mut pl, record_ref.ordinal).map_err(|e| e.to_string())?;

            let mut muts = vec![Mutation::DeleteRecordHeader {
                ordinal: record_ref.ordinal,
                record_id: record_ref.record_id,
                label: hdr.label,
            }];
            muts.append(&mut pl.mutations);

            let mut batch = empty_batch(tenant, ci);
            batch.mutations = normalize_mutations(muts);
            batch.preconditions = std::mem::take(&mut pl.preconditions);
            Ok((batch, ()))
        })
    }

    /// Remove every tenant-scoped key (records, relationships, occurrences,
    /// families/definitions, postings, labels, vectors, text docs, meta
    /// blobs, events, counters). The global commit log and outcome keys are
    /// preserved, so idempotency and replay semantics stay intact.
    ///
    /// Purge is *chunked*: each commit erases a bounded batch of keys without
    /// retaining MVCC history, and the driver loops until nothing remains
    /// (recording one terminal no-op commit when the tenant is already
    /// empty). This replaced the legacy single-commit whole-tenant purge,
    /// which materialized the entire keyspace and failed past the transaction
    /// materialization ceiling (observed at 100k-record tenants). Returns the
    /// commit index of the final purge commit. Each chunk is individually
    /// atomic; the sequence as a whole is not one transaction, so concurrent
    /// readers may observe partial erasure.
    ///
    /// # Errors
    /// Returns [`LmpgError::Conflict`] if any purge chunk loses its commit
    /// race or the store fails.
    pub fn purge_tenant(&self) -> LmpgResult<u64> {
        self.purge_tenant_cancellable(&|| false)
    }

    /// [`Self::purge_tenant`] polling `is_cancelled` between chunks, so a long
    /// purge can be aborted. Chunks already committed stay applied (the purge
    /// is resumable); the caller gets `"operation cancelled"`.
    ///
    /// # Errors
    /// Returns [`LmpgError::Conflict`] with `"operation cancelled"` when
    /// `is_cancelled` reports true, or if any purge chunk loses its commit
    /// race or the store fails.
    pub fn purge_tenant_cancellable(&self, is_cancelled: &dyn Fn() -> bool) -> LmpgResult<u64> {
        self.purge_tenant_chunked_with_cancel(PURGE_CHUNK_KEYS, is_cancelled)
    }

    /// [`Self::purge_tenant`] with an explicit chunk size (tests exercise the
    /// multi-chunk path with a small bound).
    ///
    /// # Errors
    /// Returns [`LmpgError::Conflict`] if any purge chunk loses its commit
    /// race or the store fails.
    pub fn purge_tenant_chunked(&self, max_keys: usize) -> LmpgResult<u64> {
        self.purge_tenant_chunked_with_cancel(max_keys, &|| false)
    }

    /// [`Self::purge_tenant_chunked`] polling `is_cancelled` between chunks.
    ///
    /// # Errors
    /// Returns [`LmpgError::Conflict`] with `"operation cancelled"` when
    /// `is_cancelled` reports true, or if any purge chunk loses its commit
    /// race or the store fails.
    pub fn purge_tenant_chunked_with_cancel(
        &self,
        max_keys: usize,
        is_cancelled: &dyn Fn() -> bool,
    ) -> LmpgResult<u64> {
        let chunk_usize = max_keys.max(1);
        let chunk = chunk_usize as u64;
        let tenant = self.tenant;
        loop {
            if is_cancelled() {
                return Err(LmpgError::Conflict("operation cancelled".into()));
            }
            let outcome = self
                .store
                .commit_for_tenant(&tenant, Uuid::now_v7(), |ci, txn| {
                    // The driver and the expansion share this enumeration, so
                    // "nothing left" here means the chunk has nothing to erase.
                    let remaining = expand::purge_batch_keys(txn, tenant, chunk_usize)
                        .map_err(|e| e.to_string())?
                        .len();
                    if remaining == 0 {
                        // The keyspace is empty: retire the ordinal so the
                        // directory never hands it out again.
                        let mut batch = empty_batch(tenant, ci);
                        batch.mutations = vec![Mutation::RetireTenant];
                        return Ok((batch, 0u64));
                    }
                    let mut batch = empty_batch(tenant, ci);
                    batch.mutations = vec![Mutation::PurgeTenantChunk { max_keys: chunk }];
                    Ok((batch, remaining as u64))
                });
            match outcome {
                Ok(CommitOutcome::Committed {
                    commit_index,
                    payload,
                }) => {
                    if payload == 0 {
                        if let Some((scope, _)) = self
                            .store
                            .tenant_scope(tenant)
                            .map_err(|error| LmpgError::Conflict(error.to_string()))?
                        {
                            self.store.forget_tenant(&scope);
                        }
                        return Ok(commit_index);
                    }
                }
                Ok(CommitOutcome::Conflicted(reason)) => {
                    return Err(LmpgError::Conflict(reason));
                }
                Err(error) => return Err(LmpgError::Conflict(error.to_string())),
            }
        }
    }

    // ---- relationship ops ---------------------------------------------

    /// Creates a relationship between two existing records.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if an endpoint is missing, the relationship type
    /// is denied, planning fails, or the commit conflicts.
    pub fn create_relationship(
        &self,
        rel_type: &str,
        source: RecordRef,
        target: RecordRef,
        fields: Vec<(String, FieldChange)>,
    ) -> LmpgResult<RelationshipRef> {
        self.create_relationship_tx(rel_type, source, target, fields, None)
    }

    /// Cross-service transaction variant of [`Self::create_relationship`].
    ///
    /// # Errors
    /// Returns [`LmpgError`] if an endpoint is missing, the relationship type
    /// is denied, planning fails, or the commit conflicts.
    pub fn create_relationship_tx(
        &self,
        rel_type: &str,
        source: RecordRef,
        target: RecordRef,
        fields: Vec<(String, FieldChange)>,
        tx: Option<&mut rushdb_storage::TxAccumulator>,
    ) -> LmpgResult<RelationshipRef> {
        self.create_relationship_with_id_tx(
            rel_type,
            source,
            target,
            *Uuid::now_v7().as_bytes(),
            fields,
            tx,
        )
    }

    /// Creates a relationship with a caller-supplied stable identity. This is
    /// used by logical restore/import paths that must preserve parallel edges
    /// and make retries idempotent by source relationship id.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if an endpoint is missing, the relationship type
    /// is denied, planning fails, or the identity already exists.
    pub fn create_relationship_with_id(
        &self,
        rel_type: &str,
        source: RecordRef,
        target: RecordRef,
        relationship_id: [u8; 16],
        fields: Vec<(String, FieldChange)>,
    ) -> LmpgResult<RelationshipRef> {
        self.create_relationship_with_id_tx(rel_type, source, target, relationship_id, fields, None)
    }

    fn create_relationship_with_id_tx(
        &self,
        rel_type: &str,
        source: RecordRef,
        target: RecordRef,
        relationship_id: [u8; 16],
        fields: Vec<(String, FieldChange)>,
        tx: Option<&mut rushdb_storage::TxAccumulator>,
    ) -> LmpgResult<RelationshipRef> {
        self.authorize_rel_type(rel_type)
            .map_err(LmpgError::Conflict)?;
        let tenant = self.tenant;
        let rel_type_owned = rel_type.to_string();
        let rid = relationship_id;

        run_mutation(self, tx, |ci, txn| {
            self.authorize_rel_type_in_txn(rel_type, txn, ci.saturating_sub(1))?;
            let mut pl = Planner::new(tenant, txn);
            pl.push_precondition(Precondition::exists(rushdb_types::keys::record_id_key(
                tenant,
                &source.record_id,
            )));
            pl.push_precondition(Precondition::exists(rushdb_types::keys::record_id_key(
                tenant,
                &target.record_id,
            )));
            let ordinal = pl
                .next_owner(OwnerKind::Relationship)
                .map_err(|e| e.to_string())?;

            pl.mutations.push(Mutation::CreateRelationshipHeader {
                relationship_id: rid,
                ordinal,
                rel_type: rel_type_owned.clone(),
                source_ord: source.ordinal,
                target_ord: target.ordinal,
                source_id: source.record_id,
                target_id: target.record_id,
            });

            for (name, change) in fields {
                plan_field_write(&mut pl, OwnerKind::Relationship, ordinal, &name, &change)
                    .map_err(|e| e.to_string())?;
            }

            Ok((
                finish_batch(&mut pl, ci),
                RelationshipRef {
                    relationship_id: rid,
                    ordinal,
                },
            ))
        })
    }

    /// Replaces the supplied fields on a relationship in one commit.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the relationship is missing or stale, the type
    /// is denied, or the commit conflicts.
    pub fn update_relationship_fields(
        &self,
        rel_ref: RelationshipRef,
        fields: Vec<(String, FieldChange)>,
    ) -> LmpgResult<()> {
        let tenant = self.tenant;
        let outcome = self
            .store
            .commit_for_tenant(&self.tenant, Uuid::now_v7(), |ci, txn| {
                let hdr =
                    crate::reader::try_access::relationship_by_ord(txn, tenant, rel_ref.ordinal)
                        .map_err(|e| e.to_string())?
                        .ok_or("relationship vanished")?;
                if hdr.relationship_id != rel_ref.relationship_id {
                    return Err("relationship identity mismatch".into());
                }
                self.authorize_rel_type_in_txn(&hdr.rel_type, txn, ci.saturating_sub(1))?;
                let mut pl = Planner::new(tenant, txn);
                pl.push_precondition(Precondition::exists(kx::rel_row_key(
                    tenant,
                    &rel_ref.relationship_id,
                )));
                for (name, change) in fields {
                    plan_field_write(
                        &mut pl,
                        OwnerKind::Relationship,
                        rel_ref.ordinal,
                        &name,
                        &change,
                    )
                    .map_err(|e| e.to_string())?;
                }
                Ok((finish_batch(&mut pl, ci), ()))
            });
        finalize(outcome)
    }

    /// Deletes a relationship and all of its occurrences.
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the relationship is missing, the type is
    /// denied, or the commit conflicts.
    pub fn delete_relationship(&self, rel_ref: RelationshipRef) -> LmpgResult<()> {
        self.delete_relationship_tx(rel_ref, None)
    }

    /// Cross-service transaction variant of [`Self::delete_relationship`].
    ///
    /// # Errors
    /// Returns [`LmpgError`] if the relationship is missing, the type is
    /// denied, or the commit conflicts.
    pub fn delete_relationship_tx(
        &self,
        rel_ref: RelationshipRef,
        tx: Option<&mut rushdb_storage::TxAccumulator>,
    ) -> LmpgResult<()> {
        let tenant = self.tenant;
        run_mutation(self, tx, |ci, txn| {
            let mut pl = Planner::new(tenant, txn);
            pl.push_precondition(Precondition::exists(kx::rel_row_key(
                tenant,
                &rel_ref.relationship_id,
            )));
            let raw = txn
                .get(&kx::rel_row_key(tenant, &rel_ref.relationship_id))
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "relationship vanished".to_string())?;
            let hdr: rushdb_types::occurrence::RelationshipHeader =
                serde_json::from_slice(&raw).map_err(|e| e.to_string())?;

            self.authorize_rel_type_in_txn(&hdr.rel_type, txn, ci.saturating_sub(1))?;

            stage_all_owner_occurrences(&mut pl, OwnerKind::Relationship, rel_ref.ordinal)
                .map_err(|e| e.to_string())?;

            let muts = vec![Mutation::DeleteRelationshipHeader {
                relationship_id: rel_ref.relationship_id,
                ordinal: rel_ref.ordinal,
                rel_type: hdr.rel_type.clone(),
                source_ord: hdr.source_ord,
                target_ord: hdr.target_ord,
            }];

            let mut batch = empty_batch(tenant, ci);
            let mut all = muts;
            all.append(&mut pl.mutations);
            batch.mutations = normalize_mutations(all);
            batch.preconditions = std::mem::take(&mut pl.preconditions);
            Ok((batch, ()))
        })
    }
}

fn finalize<T>(outcome: Result<CommitOutcome<T>, rushdb_storage::StorageError>) -> LmpgResult<T>
where
    T: serde::de::DeserializeOwned,
{
    match outcome {
        Ok(CommitOutcome::Committed { payload, .. }) => Ok(payload),
        Ok(CommitOutcome::Conflicted(reason)) => Err(LmpgError::Conflict(reason)),
        Err(e) => Err(LmpgError::Conflict(e.to_string())),
    }
}

/// Run a planning closure either as an autocommit (no tx) or appended to a
/// cross-service transaction's overlay (tx present). The closure is moved into
/// exactly one branch.
fn run_mutation<P, F>(
    lmpg: &Lmpg,
    tx: Option<&mut rushdb_storage::TxAccumulator>,
    build: F,
) -> LmpgResult<P>
where
    P: serde::de::DeserializeOwned + serde::Serialize,
    F: FnOnce(u64, &mut rushdb_storage::Txn<'_>) -> Result<(CommitBatch, P), String>,
{
    match tx {
        Some(tx) => lmpg
            .store
            .tx_apply(tx, Uuid::now_v7(), build)
            .map_err(|e| LmpgError::Conflict(e.to_string())),
        None => finalize(
            lmpg.store
                .commit_for_tenant(&lmpg.tenant, Uuid::now_v7(), build),
        ),
    }
}

pub(crate) fn empty_batch(tenant: rushdb_types::ids::TenantId, ci: u64) -> CommitBatch {
    CommitBatch {
        command_version: rushdb_types::command::COMMAND_VERSION,
        tenant,
        transaction_id: String::new(),
        commit_key: Uuid::nil(),
        start_index: ci.saturating_sub(1),
        isolation: IsolationMode::Snapshot,
        preconditions: vec![],
        mutations: vec![],
    }
}

fn finish_batch(pl: &mut Planner<'_>, ci: u64) -> CommitBatch {
    let mut batch = empty_batch(pl.tenant, ci);
    batch.mutations = normalize_mutations(std::mem::take(&mut pl.mutations));
    batch.preconditions = std::mem::take(&mut pl.preconditions);
    batch
}

fn normalize_mutations(mut muts: Vec<Mutation>) -> Vec<Mutation> {
    muts.sort_by_key(sort_rank);
    muts
}

fn sort_rank(m: &Mutation) -> (u8, u64) {
    match m {
        Mutation::CreateRecordHeader { ordinal, .. }
        | Mutation::CreateRelationshipHeader { ordinal, .. } => (0, *ordinal),
        Mutation::EnsureFamily { family_id, .. } => (1, *family_id),
        Mutation::EnsureDefinition { property_id, .. } => (2, *property_id),
        Mutation::DeleteOccurrence { row } => (3, row.owner_ord),
        Mutation::DeleteFamily { family_id, .. } => (3, *family_id),
        Mutation::PutOccurrence { row, .. } => (4, row.owner_ord),
        Mutation::RetypeRecordHeader { ordinal, .. } => (5, *ordinal),
        Mutation::DeleteRecordHeader { ordinal, .. }
        | Mutation::DeleteRelationshipHeader { ordinal, .. } => (9, *ordinal),
        Mutation::EnsureEmbeddingSpace { .. } => (1, u64::MAX / 2),
        Mutation::SetMetaValue { .. }
        | Mutation::SetSecurityPolicy { .. }
        | Mutation::SetSecurityPolicyThreshold { .. } => (8, 0),
        Mutation::ClearEmbeddingSpace { space_id, .. } => (8, *space_id),
        Mutation::SetMetaBlob { .. } => (8, 1),
        Mutation::DeleteMetaBlob { .. } => (8, 2),
        Mutation::PurgeTenantChunk { .. } => (8, 3),
        Mutation::RegisterTenant { .. } => (8, 7),
        Mutation::RetireTenant => (8, 8),
        Mutation::UpsertEmbeddingBinding { binding, .. } => (3, binding.vector_slot),
        Mutation::ReclaimEmbeddingSlots { .. } => (8, 4),
        Mutation::DeleteRawKey { .. } => (8, 5),
        Mutation::PutRawKey { .. } => (8, 6),
        Mutation::DefineIndex { .. }
        | Mutation::IndexBackfill { .. }
        | Mutation::DropIndex { .. }
        | Mutation::IndexDrop { .. }
        | Mutation::IndexApplied { .. } => (8, 9),
    }
}

/// Plans one field write under the document-22 resolution policy: resolve
/// or create family + definition, delete prior live occurrences of that
/// family for the owner, then stage new value occurrences.
pub(crate) fn plan_field_write(
    pl: &mut Planner<'_>,
    kind: OwnerKind,
    owner_ord: u64,
    display_name: &str,

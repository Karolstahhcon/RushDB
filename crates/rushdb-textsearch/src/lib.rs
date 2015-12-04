//! Full-text indexes for `RushDB`.
//!
//! A text index is a declared index (`IndexKind::Text`) on one record label
//! and one property — or every string property (`*`). Its tantivy index is
//! persistent, one directory per index under `data_dir/text/<tenant>/<id>/`
//! (in RAM for in-memory stores), and maintained incrementally:
//!
//! - every commit that touches a covered record leaves a `TEXT_PENDING`
//!   row (written by the expander, see `rushdb_lmpg::index`);
//! - [`TextIndexes::apply`] reads pending rows at a snapshot, re-reads each
//!   record's current text, updates tantivy (delete the owner's document,
//!   add the current one), commits tantivy, then commits `IndexApplied`, which
//!   removes exactly the rows it consumed (a change after the snapshot stays
//!   pending). Re-applying after a crash between the two commits is
//!   idempotent;
//! - an `eventual` index is searched as applied and reports its backlog; an
//!   `immediate` index applies its pending rows before searching
//!   (read-your-writes).
//!
//! Nothing here pins the WAL, and a tenant without changes never commits.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use rushdb_lmpg::index::{self, pending};
use rushdb_storage::ReadView;
use rushdb_types::ids::TenantId;
use rushdb_types::index::{IndexDef, IndexKind, IndexState, TextConsistency, TEXT_ALL_PROPERTIES};

/// Owners applied per [`TextIndexes::apply`] step.
pub const APPLY_BATCH: usize = 512;

/// Pending rows an `immediate` search applies before giving up (bounds a
/// search behind a write storm; the response then reports the backlog).
const IMMEDIATE_APPLY_LIMIT: usize = 64 * APPLY_BATCH;

/// Open tantivy indexes kept per process over a persistent store (an
/// in-memory store keeps every index open: it holds the index's only copy).
const MAX_OPEN_INDEXES: usize = 256;

/// Lexical search options.
#[derive(Debug, Clone, Copy, Default)]
pub struct LexicalOptions {
    /// Every query term must match (AND); otherwise any term may (OR).
    pub all_terms: bool,
    /// Match the whole query as a phrase (term adjacency).
    pub phrase: bool,
}

/// One BM25-ranked owner with its raw score.
#[derive(Debug, Clone)]
pub struct ScoredDoc {
    /// Record ordinal within the tenant.
    pub owner_ord: u64,
    /// Raw BM25 score; only comparable within one result set.
    pub score: f64,
}

/// One text search.
#[derive(Debug, Clone, Copy)]
pub struct TextQuery<'a> {
    /// Text index name.
    pub index: &'a str,
    /// Query string.
    pub query: &'a str,
    /// Matching options.
    pub opts: LexicalOptions,
    /// Hits to return; [`ALL_HITS`] returns every match.
    pub top_k: usize,
}

/// [`TextQuery::top_k`] that returns every match.
pub const ALL_HITS: usize = usize::MAX;

/// A search result and how far the index trails the searched snapshot.
#[derive(Debug, Clone)]
pub struct TextHits {
    /// Ranked owners.
    pub hits: Vec<ScoredDoc>,
    /// Owners matching the query (within the candidates, when given), of
    /// which `hits` are the best `top_k`.
    pub matches: u64,
    /// Changes not yet applied when the search ran (always 0 for an
    /// `immediate` index unless a write storm exceeded its apply bound).
    pub pending: u64,
}

struct OpenIndex {
    index: tantivy::Index,
    reader: tantivy::IndexReader,
    owner_field: tantivy::schema::Field,
    text_field: tantivy::schema::Field,
    /// One applier at a time per index (tantivy allows one writer).
    apply: Mutex<()>,
}

/// The process's text indexes over one store.
pub struct TextIndexes {
    store: Arc<rushdb_storage::Store>,
    open: Mutex<HashMap<(TenantId, u32), Arc<OpenIndex>>>,
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

impl TextIndexes {
    /// Text indexes over `store`.
    #[must_use]
    pub fn new(store: Arc<rushdb_storage::Store>) -> Self {
        Self {
            store,
            open: Mutex::new(HashMap::new()),
        }
    }

    /// Directory of one text index, for persistent stores.
    fn dir(&self, tenant: TenantId, id: u32) -> Option<std::path::PathBuf> {
        self.store.data_dir().map(|data| {
            data.join("text")
                .join(rushdb_types::keys::tenant_hex(tenant))
                .join(id.to_string())
        })
    }

    fn open_index(&self, tenant: TenantId, id: u32) -> Result<Arc<OpenIndex>, String> {
        use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
        let mut open = self.open.lock().map_err(|_| "text index cache poisoned")?;
        if let Some(found) = open.get(&(tenant, id)) {
            return Ok(Arc::clone(found));
        }
        let mut schema = Schema::builder();
        let owner_field = schema.add_u64_field("owner", INDEXED | STORED | FAST);
        let text_field = schema.add_text_field("text", TEXT);
        let schema = schema.build();
        let index = match self.dir(tenant, id) {
            Some(dir) => {
                std::fs::create_dir_all(&dir).map_err(err)?;
                let directory = tantivy::directory::MmapDirectory::open(&dir).map_err(err)?;
                tantivy::Index::open_or_create(directory, schema).map_err(err)?
            }
            None => tantivy::Index::create_in_ram(schema),
        };
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .map_err(err)?;
        let opened = Arc::new(OpenIndex {
            index,
            reader,
            owner_field,
            text_field,
            apply: Mutex::new(()),
        });
        // Idle on-disk indexes reopen from their files; an in-memory index
        // is its only copy, so only a persistent store evicts.
        if open.len() >= MAX_OPEN_INDEXES && self.store.data_dir().is_some() {
            open.retain(|_, index| Arc::strong_count(index) > 1);
        }
        open.insert((tenant, id), Arc::clone(&opened));
        Ok(opened)
    }

    /// Text indexes open in this process (bounded by the open-index cache).
    #[must_use]
    pub fn open_indexes(&self) -> usize {
        self.open.lock().map_or(0, |open| open.len())
    }

    /// Forget and delete the files of a text index whose definition is gone.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be removed.
    pub fn remove(&self, tenant: TenantId, id: u32) -> Result<(), String> {
        self.open
            .lock()
            .map_err(|_| "text index cache poisoned")?
            .remove(&(tenant, id));
        match self.dir(tenant, id) {
            Some(dir) if dir.exists() => std::fs::remove_dir_all(dir).map_err(err),
            _ => Ok(()),
        }
    }

    /// Forget and delete every text index of a purged tenant.
    ///
    /// # Errors
    /// Returns an error if the tenant's directory cannot be removed.
    pub fn remove_tenant(&self, tenant: TenantId) -> Result<(), String> {
        self.open
            .lock()
            .map_err(|_| "text index cache poisoned")?
            .retain(|(owner, _), _| *owner != tenant);
        match self.store.data_dir() {
            Some(data) => {
                let dir = data
                    .join("text")
                    .join(rushdb_types::keys::tenant_hex(tenant));
                if dir.exists() {
                    std::fs::remove_dir_all(dir).map_err(err)?;
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// Apply up to `max_owners` pending changes of text index `def`: index
    /// each owner's current text, commit tantivy, then commit `IndexApplied`.
    /// Returns the owners applied (0 when nothing is pending).
    ///
    /// # Errors
    /// Returns an error if a read, the tantivy commit, or the store commit
    /// fails; pending rows then stay for the next attempt.
    pub fn apply(
        &self,
        lmpg: &rushdb_lmpg::Lmpg,
        def: &IndexDef,
        max_owners: usize,
    ) -> Result<usize, String> {
        let tenant = lmpg.tenant;
        let opened = self.open_index(tenant, def.id)?;
        let _single = opened.apply.lock().map_err(|_| "text applier poisoned")?;
        let snap = lmpg.store.read_view().map_err(err)?;
        let owners = pending(&snap, tenant, def.id, max_owners).map_err(err)?;
        if owners.is_empty() {
            return Ok(0);
        }
        let mut writer: tantivy::IndexWriter = opened.index.writer(15_000_000).map_err(err)?;
        let mut session = rushdb_lmpg::reconstruct::ReconstructSession::new(&snap, tenant);
        for &owner in &owners {
            writer.delete_term(tantivy::Term::from_field_u64(opened.owner_field, owner));
            let covered = def.state.covers(owner)
                && rushdb_lmpg::reader::try_access::record_by_ord(&snap, tenant, owner)
                    .map_err(err)?
                    .is_some_and(|header| header.label == def.label);
            if !covered {
                continue;
            }
            let Some(record) = session.reconstruct_record(owner).map_err(err)? else {
                continue;

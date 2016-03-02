//! ANN projection over canonical vector segments, derived and fully
//! rebuildable. The default backing is `crate::segment::SegmentSet`
//! (`AnnBacking::Segments`): a space's `usearch`-backed segments (a hot
//! segment plus frozen, tier-merged ones, persisted when the store is),
//! reconciled with the canonical bindings when a space is loaded. The optional
//! `turbovec` feature keeps its own `AnnProvider`-backed path
//! (`AnnBacking::Provider`) for high-dimensional spaces, since its
//! quantized index predates, and does not fit, `segment::Segment`'s
//! `usearch`-specific shape. Search is filter-aware with exact rerank;
//! insufficient ANN candidates fall back to exact scoring under the same
//! pinned view. Canonical storage corruption is an error, not an empty
//! generation.

/// Entries sampled to estimate filter selectivity for the filtered-ANN gather
/// size (Slice 27 / 11-filter-aware-ann.md). 256 is a stable estimator and is
/// far cheaper than the gather itself. Sampling is spread across the
/// generation rather than using a potentially biased prefix.
const FILTERED_SELECTIVITY_SAMPLE: usize = 256;

/// Entry count at which background maintenance freezes a space's hot
/// segment (brute-force scanned) into a frozen one, and the base size of the
/// merge tiers.
pub(crate) const HOT_SEGMENT_FREEZE_THRESHOLD: usize = 256;

/// The absorb path freezes the hot segment itself only past this size, a
/// backstop for maintenance falling behind; normally freezing and merging
/// run off the write path.
const HOT_SEGMENT_BACKSTOP: usize = 4 * HOT_SEGMENT_FREEZE_THRESHOLD;

/// Largest segment one build step writes when a space is (re)built from
/// its bindings: the budget usually bounds the chunk first; this
/// bounds how long one step holds the space's build gate.
const MAX_BUILD_CHUNK: usize = 256_000;

/// ANN searches answered exactly because the ANN budget could not admit the
/// space's index (a legitimate degradation, exported as
/// `rushdb_ann_exact_fallbacks_total`).
static ANN_EXACT_FALLBACKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Cumulative [`ANN_EXACT_FALLBACKS`] of this process.
#[must_use]
pub fn ann_exact_fallbacks_total() -> u64 {
    ANN_EXACT_FALLBACKS.load(std::sync::atomic::Ordering::Relaxed)
}

/// `(prev_ci, ci)` of a space's binding set: the commit that last
/// changed its bindings and the one before. `(0, 0)` for a space never bound.
pub(crate) fn space_version<R: TryRead>(
    r: &R,
    t: TenantId,
    space_id: u64,
) -> Result<(u64, u64), VectorError> {
    match r.get(&rushdb_types::keys::emb_space_version_key(t, space_id))? {
        None => Ok((0, 0)),
        Some(raw) => rushdb_types::keys::parse_emb_space_version(&raw)
            .ok_or(VectorError::Storage(StorageError::CorruptLog)),
    }
}

use rushdb_lmpg::reader::TryRead;
use rushdb_storage::{ReadView, StorageError};
use rushdb_types::ids::TenantId;
use rushdb_types::keys::emb_vector_key;
use rushdb_types::occurrence::BindingStatus;

use std::collections::HashMap;
use std::sync::Arc;

use crate::search::{FallbackReason, SearchPath};
use crate::{ScoredSource, SourceRef, VectorEngine, VectorError};

/// A rebuildable ANN projection of one Space's canonical vector segments.
#[derive(Debug, Clone)]
pub struct AnnGeneration {
    /// Space this generation was built for.
    pub space_id: u64,
    /// Reserved pivot vectors; currently always empty for the HNSW provider.
    pub pivots: Vec<Vec<f32>>,
    /// assignment list: (slot index into pivot posting)
    pub assignments: Vec<(u64, usize)>,
    /// One entry per canonical vector, in slot order.
    pub entries: Vec<AnnEntry>,
}

/// Durable description of a rebuildable ANN generation. The provider graph is
/// deliberately not serialized yet; `providerState` exposes that distinction
/// to callers after restart.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnManifest {
    /// On-disk schema version of this manifest.
    pub format_version: u8,
    /// Commit index the generation was built from.
    pub source_commit_index: u64,
    /// Fingerprint of the input entries, compared for freshness.
    pub input_fingerprint: String,
    /// Number of entries in the generation.
    pub entries: usize,
    /// Name of the ANN provider that built the generation.
    pub provider: String,
}

/// One canonical vector admitted to a generation: its storage slot, the
/// source it projects, and the value hash recorded at bind time.
#[derive(Debug, Clone)]
pub struct AnnEntry {
    /// Vector-segment slot in the space's canonical storage.
    pub slot: u64,
    /// Canonical source this vector projects.
    pub source: SourceRef,
    /// Pivot assignment; currently always zero for the HNSW provider.
    pub pivot: usize,
    /// Hash of the source value at bind time.
    pub value_hash: u64,
}

pub(crate) struct CachedAnn {
    /// The space version (commit that last changed its bindings) this
    /// generation reflects. Current iff it equals the version row at the
    /// reader's view: an O(1) check that no other space's or tenant's commit
    /// can disturb.
    pub space_version: u64,
    pub dims: usize,
    pub generation: AnnGeneration,
    pub backing: AnnBacking,
    /// Shared logical-cache reservation held for the lifetime of this cached
    /// generation. The estimate includes provider overhead headroom so a
    /// rebuild cannot allocate an unaccounted corpus-sized spike.
    pub _reservation: Option<rushdb_storage::resources::ByteReservation>,
}

/// What actually answers a gather for one cached generation.
/// `Segments` — `crate::segment::SegmentSet` — is the live default path. `Provider` only remains for
/// `turbovec`: its `AnnProvider` impl predates, and does not fit,
/// `segment::Segment`'s shape (a `usearch`-specific mmap-eligible frozen
/// tier), so the high-dimensional quantized path keeps its own mechanism
/// rather than being forced into one it doesn't need yet.
pub(crate) enum AnnBacking {
    /// Shared via `Arc` (not owned outright) so an incremental bind can
    /// mutate the same segment set a reader already holds through a
    /// cached generation snapshot — see
    /// `VectorEngine::incrementally_absorb_bind`.
    Segments(Arc<crate::segment::SegmentSet>),
    #[cfg(feature = "turbovec")]
    Provider(Box<dyn AnnProvider>),
}

impl AnnBacking {
    fn name(&self) -> &'static str {
        match self {
            AnnBacking::Segments(_) => "usearch",
            #[cfg(feature = "turbovec")]
            AnnBacking::Provider(provider) => provider.name(),
        }
    }
}

const MAX_CACHED_ANN_BYTES: usize = 256 * 1024 * 1024;

/// The in-memory LRU/eviction accounting (`MAX_CACHED_ANN_BYTES`) must use
/// the same real measured size as the `ByteBudget` reservation:
/// otherwise eviction could believe there is room (formula-estimated sum
/// under budget) while the real, charged reservation is already exhausted.
pub(crate) fn cached_ann_bytes(cached: &CachedAnn) -> usize {
    measured_backing_bytes(&cached.backing, &cached.generation, cached.dims)
}

#[cfg(feature = "turbovec")]
fn generation_bytes(generation: &AnnGeneration, dims: usize) -> usize {
    generation
        .entries
        .len()
        .saturating_mul(
            dims.saturating_mul(std::mem::size_of::<f32>())
                .saturating_add(64),
        )
        .saturating_add(generation.pivots.iter().map(Vec::len).sum::<usize>() * 4)
}

/// Real, measured resident bytes for an already-built backing,
/// replacing `generation_bytes`'s pre-build formula estimate with the actual
/// footprint: `usearch::Index::memory_usage()` for every frozen segment (a
/// real introspection the provider exposes) plus the hot segment's own
/// owned `Vec` bytes. `turbovec`'s `Provider` has no such introspection on
/// `AnnProvider`, so it keeps the formula estimate — a narrower, documented
/// fallback, not the common path (`Segments` is the live default for every
/// space since Bucket 9b).
#[cfg_attr(
    not(feature = "turbovec"),
    expect(
        unused_variables,
        reason = "generation/dims only feed the turbovec formula-estimate fallback arm"
    )
)]
fn measured_backing_bytes(backing: &AnnBacking, generation: &AnnGeneration, dims: usize) -> usize {
    match backing {
        AnnBacking::Segments(set) => set.measured_bytes(),
        #[cfg(feature = "turbovec")]
        AnnBacking::Provider(_) => generation_bytes(generation, dims).saturating_mul(2),
    }
}

fn vector_cache_budget(
    store: &std::sync::Arc<rushdb_storage::Store>,
) -> Result<rushdb_storage::resources::ByteBudget, VectorError> {
    // A resident ANN build must be byte-accounted. With the logical cache
    // disabled (`resources.logical-cache-bytes: 0`) no ANN may be resident,
    // so this is a budget refusal: search answers exactly, as it does when
    // a build does not fit.
    store.vector_cache_budget().ok_or_else(|| {
        VectorError::AnnBudget("no logical cache budget (resources.logical-cache-bytes: 0)".into())
    })
}

/// Reserves the exact real footprint of an already-built `backing`:
/// used by every caller that builds the backing before reserving
/// (`admit_fresh_ann` and the incremental-absorb paths), where the real
/// number is already known and a formula estimate would just be wrong.
fn reserve_generation(
    store: &std::sync::Arc<rushdb_storage::Store>,
    backing: &AnnBacking,
    generation: &AnnGeneration,
    dims: usize,
) -> Result<Option<rushdb_storage::resources::ByteReservation>, VectorError> {
    let budget = vector_cache_budget(store)?;
    let bytes = measured_backing_bytes(backing, generation, dims);
    budget
        .try_reserve(bytes)
        .map(Some)
        .map_err(|error| VectorError::AnnBudget(error.to_string()))
}

/// Release a cache entry no successor shares segments with (an explicit
/// full rebuild, a cleared space): its segment files go once the last
/// in-flight reader drops them. Eviction keeps them: the next
/// miss reopens them.
pub(crate) fn retire_cached(entry: &CachedAnn) {
    #[allow(
        irrefutable_let_patterns,
        reason = "refutable only with --features turbovec, which adds AnnBacking::Provider"
    )]
    if let AnnBacking::Segments(set) = &entry.backing {
        set.supersede_all();
    }
}

/// Install a generation for `id`. `retire_old` only when the replaced one
/// shares no segment file with it (an explicit full rebuild); a reopened
/// generation maps the same files as the one it replaces.
fn replace_cached(
    cache: &mut HashMap<u64, Arc<CachedAnn>>,
    id: u64,
    entry: Arc<CachedAnn>,
    retire_old: bool,
) {
    if let Some(old) = cache.remove(&id) {
        if retire_old {
            retire_cached(&old);
        }
    }
    admit_ann(cache, id, entry);
}

fn admit_ann(cache: &mut HashMap<u64, Arc<CachedAnn>>, id: u64, entry: Arc<CachedAnn>) {
    let bytes = cached_ann_bytes(&entry);
    while cache.len() >= crate::MAX_CACHED_ANN_GENERATIONS
        || cache
            .values()
            .map(|cached| cached_ann_bytes(cached))
            .sum::<usize>()
            .saturating_add(bytes)
            > MAX_CACHED_ANN_BYTES
    {
        let Some(idle) = cache
            .iter()
            .find_map(|(id, cached)| (Arc::strong_count(cached) == 1).then_some(*id))
        else {
            break;
        };
        // Its files stay for the next miss to reopen.
        cache.remove(&idle);
    }
    if bytes <= MAX_CACHED_ANN_BYTES
        && cache
            .values()
            .map(|cached| cached_ann_bytes(cached))
            .sum::<usize>()
            .saturating_add(bytes)
            <= MAX_CACHED_ANN_BYTES
        && cache.len() < crate::MAX_CACHED_ANN_GENERATIONS
    {
        cache.insert(id, entry);
    }
}

/// ANN index abstraction. Each provider builds an index over canonical
/// vectors (in `entries` slot order) and answers approximate nearest
/// neighbours by gathering candidate entry indices for a query. Only
/// `turbovec` implements this now (the default path builds a
/// `segment::FrozenSegment` directly instead).
#[cfg(feature = "turbovec")]
pub(crate) trait AnnProvider: Send + Sync {
    /// Build an index over the given canonical vectors (one entry per vector).
    fn build(vectors: &[Vec<f32>]) -> Result<Self, String>
    where
        Self: Sized;

    /// Name of the provider, for observability/tests.
    fn name(&self) -> &'static str;

    /// Gather up to `k` candidate entry indices (approximate, best-first).
    /// `ef` is the beam-width hint: HNSW honours it; flat indexes ignore it.
    fn gather(&self, query: &[f32], k: usize, ef: usize) -> Vec<usize>;
}

/// `TurboQuant` provider (Google Research), gated behind the `turbovec`
/// feature. Data-oblivious 2-4 bit quantization with no training and SIMD
/// search; used only for high-dimensional spaces where its recall is strong.
#[cfg(feature = "turbovec")]
struct TurboProvider {
    index: turbovec::TurboQuantIndex,
}

#[cfg(feature = "turbovec")]
impl AnnProvider for TurboProvider {
    fn build(vectors: &[Vec<f32>]) -> Result<Self, String> {
        let dims = vectors.first().map_or(0, Vec::len);
        let bit_width = 4;
        let mut index = turbovec::TurboQuantIndex::new(dims, bit_width)
            .map_err(|e| format!("turbovec::new: {e}"))?;
        let mut flat = Vec::with_capacity(vectors.len() * dims);
        for v in vectors {
            flat.extend_from_slice(v);
        }
        index
            .add_2d(&flat, dims)
            .map_err(|e| format!("turbovec::add_2d: {e}"))?;
        index.prepare();
        Ok(TurboProvider { index })
    }

    fn name(&self) -> &'static str {
        "turbovec"
    }

    fn gather(&self, query: &[f32], k: usize, _ef: usize) -> Vec<usize> {
        self.index
            .search(query, k)
            .indices_for_query(0)
            .iter()
            .filter_map(|&i| usize::try_from(i).ok())
            .collect()
    }
}

/// Canonical-source identity for deduping `AnnGeneration.entries` by
/// source, mirroring `segment::source_key` (private to that module, so not
/// reused directly).
fn ann_source_key(source: SourceRef) -> (u8, u64, u64, u64) {
    (
        source.kind.as_byte(),
        source.owner_ord,
        source.pid,
        source.elem,
    )
}

/// Whether a space of `dims` is served by the `turbovec` provider instead of
/// segments (high-dimensional spaces, with the feature only).
fn wants_provider(dims: usize) -> bool {
    cfg!(feature = "turbovec") && dims >= 256 && dims.is_multiple_of(8)
}

/// Bytes a build of `n` vectors of `dims` holds before it is measured:
/// the per-vector index footprint ([`crate::segment::build_bytes_per_vector`])
/// for segments; the canonical f32 corpus plus graph headroom for the
/// provider, which decodes the corpus whole.
fn build_estimate(n: usize, dims: usize) -> usize {
    let per_vector = if wants_provider(dims) {
        dims.saturating_mul(std::mem::size_of::<f32>())
            .saturating_add(64)
            .saturating_mul(2)
    } else {
        crate::segment::build_bytes_per_vector(u32::try_from(dims).unwrap_or(u32::MAX))
    };
    n.saturating_mul(per_vector)
}

impl VectorEngine {
    /// The directory a space's persisted segments live in, when a segments
    /// directory is configured (`None` for a pure in-memory store).
    fn space_dir(&self, space_id: u64) -> Option<std::path::PathBuf> {
        self.segments_dir
            .as_ref()
            .map(|dir| dir.join(format!("space-{space_id}")))
    }

    /// A fresh path for one of this space's segments, when a segments
    /// directory is configured. Unique across sets and engine instances (a
    /// superseded segment's file is deleted when its last reader drops it,
    /// which must never hit a newer segment that reused the name), and time-ordered (`uuid` v7), so a directory listing is the
    /// segments' creation order ([`crate::segment::open_space_dir`]).
    fn space_segment_path(&self, space_id: u64) -> Option<std::path::PathBuf> {
        self.space_dir(space_id)
            .map(|dir| dir.join(format!("seg-{}", uuid::Uuid::now_v7().simple())))
    }

    fn ann_manifest_key(&self, space_id: u64) -> String {
        format!(
            "{}{space_id}",
            rushdb_types::keys::ann_manifest_meta_prefix(self.tenant())
        )
    }

    fn generation_fingerprint(generation: &AnnGeneration) -> String {
        let mut hash = blake3::Hasher::new();
        for entry in &generation.entries {
            hash.update(&entry.slot.to_be_bytes());
            hash.update(&entry.value_hash.to_be_bytes());
            hash.update(&[entry.source.kind.as_byte()]);
            hash.update(&entry.source.owner_ord.to_be_bytes());
            hash.update(&entry.source.pid.to_be_bytes());
            hash.update(&entry.source.elem.to_be_bytes());
        }
        hash.finalize().to_hex().to_string()
    }

    /// Fingerprint the current ready bindings **without decoding any vector
    /// payloads**. Metadata callers (`list`, `stats`, `create`, inline-vector
    /// validation) must not materialize the whole corpus just to decide whether
    /// a manifest is current; the fingerprint is defined over binding metadata
    /// only, so this is identical to `generation_fingerprint` of the built
    /// generation but O(bindings) JSON parses instead of O(corpus) f32 decodes.
    fn generation_fingerprint_in_view(
        r: &impl TryRead,
        t: TenantId,
        space: &crate::EmbeddingSpace,
    ) -> Result<String, VectorError> {
        let bindings = load_ready_bindings(r, t, space.space_id)?;
        let mut hash = blake3::Hasher::new();
        for b in &bindings {
            hash.update(&b.vector_slot.to_be_bytes());
            hash.update(&b.source_hash.to_be_bytes());
            hash.update(&[b.owner_kind.as_byte()]);
            hash.update(&b.owner_ord.to_be_bytes());
            hash.update(&b.pid.to_be_bytes());
            hash.update(&b.elem.to_be_bytes());
        }
        Ok(hash.finalize().to_hex().to_string())
    }

    /// Report persistent-manifest freshness separately from the ephemeral
    /// provider cache. A restart therefore says `rebuildRequired` until the
    /// provider has been rebuilt, even when canonical bindings are current.
    ///
    /// # Errors
    ///
    /// Returns [`VectorError::Storage`] when `snap` belongs to another store
    /// or a manifest or vector segment is corrupt, and
    /// [`VectorError::Engine`] when the ANN cache mutex is poisoned.
    pub fn ann_lifecycle_in_view(
        &self,
        snap: &ReadView,
        space: &crate::EmbeddingSpace,
    ) -> Result<serde_json::Value, VectorError> {
        self.validate_view(snap)?;
        let raw = snap.get(self.ann_manifest_key(space.space_id).as_bytes())?;
        let manifest: Option<AnnManifest> = raw
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .map_err(|_| VectorError::Storage(StorageError::CorruptLog))
            })
            .transpose()?;
        let current_fingerprint = Self::generation_fingerprint_in_view(snap, self.tenant(), space)?;
        let manifest_current = manifest
            .as_ref()
            .is_some_and(|manifest| manifest.input_fingerprint == current_fingerprint);
        let provider_cached = self
            .ann_cache
            .lock()
            .map_err(|_| VectorError::Engine("ANN cache mutex poisoned".into()))?
            .get(&space.space_id)
            .is_some_and(|cached| {
                Self::generation_fingerprint(&cached.generation) == current_fingerprint
            });
        Ok(serde_json::json!({
            "manifestState": if manifest_current { "current" } else { "rebuildRequired" },
            "providerState": if provider_cached { "memoryCached" } else { "rebuildRequired" },
            "manifest": manifest,
        }))
    }
    pub(super) fn current_ann(
        &self,
        snap: &ReadView,
        space: &crate::EmbeddingSpace,
    ) -> Result<Arc<CachedAnn>, VectorError> {
        let (_, version) = space_version(snap, self.tenant(), space.space_id)?;
        let cached = |engine: &Self| -> Result<Option<Arc<CachedAnn>>, VectorError> {
            Ok(engine
                .ann_cache
                .lock()
                .map_err(|_| VectorError::Engine("ANN cache mutex poisoned".into()))?
                .get(&space.space_id)
                .filter(|cached| cached.space_version == version)
                .cloned())
        };
        if let Some(cached) = cached(self)? {
            return Ok(cached);
        }
        // One assembly per space at a time: concurrent misses wait
        // for it instead of each decoding the corpus.
        let gate = Arc::clone(
            self.ann_builds
                .lock()
                .map_err(|_| VectorError::Engine("ANN build gate poisoned".into()))?
                .entry(space.space_id)
                .or_default(),
        );
        let _building = gate
            .lock()
            .map_err(|_| VectorError::Engine("ANN build gate poisoned".into()))?;
        if let Some(cached) = cached(self)? {
            return Ok(cached);
        }
        let (generation, backing, reservation) = self.assemble(snap, space, true)?;
        self.admit_fresh_ann(
            space.space_id,
            version,
            generation,
            backing,
            space.dims as usize,
            reservation,
        )
    }

    /// Builds a space's backing from its current bindings at `snap`
    ///. With `reopen`, the space's persisted segments are reopened
    /// and reconciled first, so only bindings no segment covers are decoded
    /// (after a restart or a tenant eviction, usually none or the unfrozen
    /// tail). Every decode is reserved before it happens: the rest is built
    /// in chunks sized to the free vector budget, each persisted as its own
    /// segment and released before the next, so a space far larger than the
    /// budget is still admitted when its segments are file-backed. A chunk
    /// that cannot be reserved is an `AnnBudget` refusal (the search answers
    /// exactly); the chunks already persisted are reopened by the next try.
    fn assemble(
        &self,
        snap: &ReadView,
        space: &crate::EmbeddingSpace,
        reopen: bool,
    ) -> Result<
        (
            AnnGeneration,
            AnnBacking,
            Option<rushdb_storage::resources::ByteReservation>,
        ),
        VectorError,
    > {
        let tenant = self.tenant();
        let dims = space.dims as usize;
        let bindings = load_ready_segment_entries(snap, tenant, space.space_id)?;
        let generation = generation_of(space.space_id, &bindings);
        let budget = vector_cache_budget(&self.store)?;
        let refuse = |error: rushdb_storage::resources::ResourceError| {
            VectorError::AnnBudget(error.to_string())
        };
        let decode = |slot: u64| {
            let raw = snap.get(&emb_vector_key(tenant, space.space_id, slot))?;
            crate::decode_vector(raw, space.dims)
        };
        #[cfg(feature = "turbovec")]
        if wants_provider(dims) {
            let mut reservation = budget
                .try_reserve(build_estimate(bindings.len(), dims))
                .map_err(refuse)?;
            let vectors: Vec<Vec<f32>> = bindings
                .iter()
                .map(|entry| decode(entry.slot))
                .collect::<Result<_, _>>()?;
            if let Ok(provider) = TurboProvider::build(&vectors) {
                let backing = AnnBacking::Provider(Box::new(provider));
                reservation
                    .resize(measured_backing_bytes(&backing, &generation, dims))
                    .map_err(refuse)?;
                return Ok((generation, backing, Some(reservation)));
            }
        }
        let mut reservation = budget.try_reserve(0).map_err(refuse)?;
        let set = crate::segment::SegmentSet::new();
        let space_dir = self.space_dir(space.space_id);
        if let (true, Some(dir)) = (reopen, space_dir.as_deref()) {
            for segment in crate::segment::open_space_dir(dir, space.dims, space.similarity)? {
                set.push(Arc::new(crate::segment::Segment::Frozen(segment)));
            }
        }
        let mut missing = set.reconcile(&bindings);
        reservation.resize(set.measured_bytes()).map_err(refuse)?;
        if set.frozen_count() > 0 && missing.len() < HOT_SEGMENT_FREEZE_THRESHOLD {
            // A small tail next to reopened segments stays hot (f32,
            // exactly measured afterwards) until maintenance freezes it.
            let tail = missing.len().saturating_mul(
                dims.saturating_mul(std::mem::size_of::<f32>())
                    .saturating_add(64),
            );
            reservation
                .resize(set.measured_bytes().saturating_add(tail))
                .map_err(refuse)?;
            for entry in std::mem::take(&mut missing) {
                let vector = decode(entry.slot)?;
                set.push_to_hot(entry, vector);
            }
        }
        while !missing.is_empty() {
            let fits = budget.available() / build_estimate(1, dims).max(1);
            let chunk = fits.min(missing.len()).min(MAX_BUILD_CHUNK);
            if chunk < HOT_SEGMENT_FREEZE_THRESHOLD.min(missing.len()) {
                return Err(VectorError::AnnBudget(format!(
                    "{} vectors left to index; the vector budget admits {fits} at a time",
                    missing.len()
                )));
            }
            let rest = missing.split_off(chunk);
            let estimate = build_estimate(missing.len(), dims);
            reservation
                .resize(set.measured_bytes().saturating_add(estimate))
                .map_err(refuse)?;
            let entries = std::mem::replace(&mut missing, rest);
            #[expect(
                clippy::needless_collect,
                reason = "the build takes `entries` by value while the vectors stream"
            )]
            let slots: Vec<u64> = entries.iter().map(|entry| entry.slot).collect();
            let vectors = slots.into_iter().map(decode);
            let frozen = match self.space_segment_path(space.space_id) {
                Some(path) => crate::segment::FrozenSegment::build(
                    entries,
                    vectors,
                    space.dims,
                    space.similarity,
                    &path,
                ),
                None => crate::segment::FrozenSegment::build_in_memory(
                    entries,
                    vectors,
                    space.dims,
                    space.similarity,
                ),
            }?;
            set.push(Arc::new(crate::segment::Segment::Frozen(frozen)));
            reservation.resize(set.measured_bytes()).map_err(refuse)?;
        }
        let backing = AnnBacking::Segments(Arc::new(set));
        reservation
            .resize(measured_backing_bytes(&backing, &generation, dims))
            .map_err(refuse)?;
        Ok((generation, backing, Some(reservation)))
    }

    /// Shared tail of every "this space's generation was assembled" path:
    /// wrap it as a `CachedAnn` and admit it into the cache unless a newer
    /// generation already won the race. The replaced generation is not
    /// retired: a reopened one maps the same files.
    fn admit_fresh_ann(
        &self,
        space_id: u64,
        space_version: u64,
        generation: AnnGeneration,
        backing: AnnBacking,
        dims: usize,
        reservation: Option<rushdb_storage::resources::ByteReservation>,
    ) -> Result<Arc<CachedAnn>, VectorError> {
        let cached = Arc::new(CachedAnn {
            space_version,
            dims,
            generation,
            backing,
            _reservation: reservation,
        });
        let mut cache = self
            .ann_cache
            .lock()
            .map_err(|_| VectorError::Engine("ANN cache mutex poisoned".into()))?;
        if cache
            .get(&space_id)
            .is_none_or(|entry| entry.space_version <= space_version)
        {
            replace_cached(&mut cache, space_id, Arc::clone(&cached), false);
        }
        Ok(cached)
    }

    /// Keeps a space's cached ANN generation current after a bind commit
    /// instead of leaving the next read to rebuild it. Best-effort: does
    /// nothing unless the commit's space version row chains onto the cached
    /// one (`prev_ci == cached.space_version` and `ci == commit_index`) —
    /// commits to other spaces or tenants never break the chain —
    /// and every absorbed bind is `Ready` at that commit. Skipping is always
    /// safe: the next read sees a different space version and rebuilds.
    ///
    /// Absorb only appends to the hot segment; freezing and merging happen
    /// in [`Self::maintain_segments`], off the write path.
    pub(crate) fn incrementally_absorb_binds(
        &self,
        space_id: u64,
        commit_index: u64,
        binds: Vec<(AnnEntry, Vec<f32>)>,
        dims: u32,
        similarity: rushdb_types::occurrence::Similarity,
    ) {
        if binds.is_empty() {
            return;
        }
        let Ok(view) = self.store.read_view() else {
            return;
        };
        let tenant = self.tenant();
        let Ok((prev, version)) = space_version(&view, tenant, space_id) else {
            return;
        };
        if version != commit_index {
            return;
        }
        // `expect_live_hash` binds may have been stored as stale/retired.
        let all_ready = binds.iter().all(|(entry, _)| {
            view.get(&rushdb_types::keys::emb_binding_key(
                tenant,
                space_id,
                entry.source.kind,
                entry.source.owner_ord,
                entry.source.pid,
                entry.source.elem,
            ))
            .ok()
            .flatten()
            .and_then(|raw| {
                serde_json::from_slice::<rushdb_types::occurrence::EmbeddingBinding>(&raw).ok()
            })
            .is_some_and(|binding| binding.status == BindingStatus::Ready)
        });
        drop(view);
        if !all_ready {
            return;
        }
        let Ok(mut cache) = self.ann_cache.lock() else {
            return;
        };
        let Some(cached) = cache.get(&space_id).cloned() else {
            return;
        };
        if cached.space_version != prev {
            return;
        }
        #[allow(
            irrefutable_let_patterns,
            reason = "refutable only with --features turbovec, which adds AnnBacking::Provider"
        )]
        let AnnBacking::Segments(set) = &cached.backing
        else {
            return;
        };
        // A fork, never the set an in-flight reader may be searching.
        let new_set = set.fork();
        let mut entries = cached.generation.entries.clone();
        for (entry, vector) in binds {
            let segment_entry = crate::segment::SegmentEntry {
                slot: entry.slot,
                source: entry.source,
                value_hash: entry.value_hash,
            };
            new_set.push_to_hot(segment_entry, vector);
            // `generation.entries` is a heuristic-only list (emptiness,
            // `requested` bound, selectivity sample): dedup by source so
            // rebinds cannot grow it without bound.
            entries.retain(|e| ann_source_key(e.source) != ann_source_key(entry.source));
            entries.push(entry);
        }
        // Backstop only: maintenance normally freezes at the threshold.
        if new_set.newest_hot_len() >= HOT_SEGMENT_BACKSTOP {
            let path = self.space_segment_path(space_id);
            let _ = new_set.freeze_newest_hot(dims, similarity, path.as_deref());
        }
        let assignments = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.slot, i))
            .collect();
        let generation = AnnGeneration {
            space_id,
            pivots: vec![],
            assignments,
            entries,
        };
        let backing = AnnBacking::Segments(Arc::new(new_set));
        let Ok(reservation) = reserve_generation(&self.store, &backing, &generation, cached.dims)
        else {
            // Budget exhausted for the grown generation: keep the older
            // entry; the next read sees the newer version and rebuilds.
            return;
        };
        cache.insert(
            space_id,
            Arc::new(CachedAnn {
                space_version: commit_index,
                dims: cached.dims,
                generation,
                backing,
                _reservation: reservation,
            }),
        );
    }

    /// The retirement-side counterpart of [`Self::incrementally_absorb_binds`]:
    /// removes `retired` from the live map of the cached set. Same version
    /// chain. A commit that also marked bindings stale (`stale > 0`) is not
    /// absorbed: its stale sources are not reported, so the next read
    /// rebuilds.
    pub(crate) fn incrementally_absorb_retirements(
        &self,
        space_id: u64,
        commit_index: u64,
        retired: &[SourceRef],
        stale: usize,
    ) {
        if retired.is_empty() || stale > 0 {
            return;
        }
        let Ok(view) = self.store.read_view() else {
            return;
        };
        let Ok((prev, version)) = space_version(&view, self.tenant(), space_id) else {
            return;
        };
        drop(view);
        if version != commit_index {
            return;
        }
        let Ok(mut cache) = self.ann_cache.lock() else {
            return;
        };
        let Some(cached) = cache.get(&space_id).cloned() else {
            return;
        };
        if cached.space_version != prev {
            return;
        }
        #[allow(
            irrefutable_let_patterns,
            reason = "refutable only with --features turbovec, which adds AnnBacking::Provider"
        )]
        let AnnBacking::Segments(set) = &cached.backing
        else {
            return;
        };
        let new_set = set.fork();
        for &source in retired {
            new_set.tombstone(source);
        }
        let retired_keys: std::collections::HashSet<(u8, u64, u64, u64)> =
            retired.iter().map(|&s| ann_source_key(s)).collect();
        let mut entries = cached.generation.entries.clone();
        entries.retain(|e| !retired_keys.contains(&ann_source_key(e.source)));
        let assignments = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.slot, i))
            .collect();
        let generation = AnnGeneration {
            space_id,
            pivots: vec![],
            assignments,
            entries,
        };
        let backing = AnnBacking::Segments(Arc::new(new_set));
        let Ok(reservation) = reserve_generation(&self.store, &backing, &generation, cached.dims)
        else {
            return;
        };
        cache.insert(
            space_id,
            Arc::new(CachedAnn {
                space_version: commit_index,
                dims: cached.dims,
                generation,
                backing,
                _reservation: reservation,
            }),
        );
    }

    /// Background segment maintenance for every cached space: freeze
    /// a hot segment that reached [`HOT_SEGMENT_FREEZE_THRESHOLD`], then run
    /// at most one size-tiered merge (reading the live entries' canonical
    /// vectors from a current snapshot). The work happens on a fork, outside
    /// the cache lock; it is published only if the cache entry is still the
    /// one it started from (`Arc::ptr_eq`), otherwise it is discarded and the
    /// next tick retries. Returns the number of spaces changed.

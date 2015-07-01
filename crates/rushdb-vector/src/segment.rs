//! Multi-segment ANN architecture (tenancy buckets 9a/9b): one
//! hot segment (brute-force, absorbs new writes) plus N immutable frozen
//! segments (`usearch`-backed, mmap-eligible). [`crate::ann`]'s
//! `AnnBacking::Segments` is the live default path for every space; see
//! `docs/manual/architecture/vector-internals.md` for the design this
//! implements.
//!
//! Corrected read path (the blueprint's §5.3, replacing the rejected
//! per-segment-concat design): gather candidates per segment, resolve each
//! candidate's canonical source to its **newest** segment (an update in a
//! later segment supersedes an older one — this is what makes the design
//! correct under multiple segments, where the single-generation path never
//! had to consider it), then exact-rerank the deduped set from canonical
//! vectors.
//!
//! **Bucket 9b, fully landed:** `HotSegment::push` absorbs a single bind
//! incrementally (`ann::VectorEngine::incrementally_absorb_bind`), without
//! rebuilding the whole space from canonical bindings; `SegmentSet`'s
//! tombstone set (below) removes a source from search results independent
//! of a rebind, without touching any segment's physical index; and
//! `FrozenSegment::build`/`open` persist/reopen a frozen segment via
//! `usearch`'s mmap `.view()` under the store's data directory when one is
//! configured (`ann::VectorEngine::with_segments_dir`), falling back to
//! `build_in_memory` for a pure in-memory store. A tombstoned source is
//! simply absent from `search`'s resolved candidates, never from a
//! segment's own entries.
//!
//! **Reopen:** every persisted segment carries its entries in a
//! sidecar, so a cache miss (restart, tenant eviction) reopens the space's
//! whole directory ([`open_space_dir`]) and [`SegmentSet::reconcile`]s it
//! against the current bindings: an entry stays live only if it is its
//! source's current binding, and only bindings no segment covers are
//! decoded again.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use rushdb_types::occurrence::Similarity;
#[cfg(test)]
use std::collections::HashMap;

use crate::{ScoredSource, SourceRef, VectorError};

/// On-disk schema version of a frozen segment's manifest. Bump and
/// rebuild-on-mismatch on any incompatible change, the same discipline
/// `ann::AnnManifest::format_version` already applies.
const SEGMENT_FORMAT_VERSION: u8 = 2;

/// One canonical vector admitted to a segment: its storage slot, the source
/// it projects, and the value hash recorded at bind time. Mirrors
/// `ann::AnnEntry`; kept separate because segments have their own identity
/// (position within ONE segment, not a single generation's slot order).
#[derive(Debug, Clone)]
pub(crate) struct SegmentEntry {
    pub slot: u64,
    pub source: SourceRef,
    pub value_hash: u64,
}

/// Durable description of a frozen segment's persisted provider file.
/// Reused only when `input_fingerprint` matches the canonical bindings it
/// was built from; otherwise the segment is a cache miss and must be
/// rebuilt, never treated as data loss (the canonical vectors are the
/// source of truth).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SegmentManifest {
    pub format_version: u8,
    pub input_fingerprint: String,
    pub entries: usize,
    pub dims: u32,
}

/// Fingerprints a segment's entries the same way as
/// `ann::generation_fingerprint`, so a frozen segment's reconciliation
/// check is defined identically to the single-generation path's.
pub(crate) fn segment_fingerprint(entries: &[SegmentEntry]) -> String {
    let mut hash = blake3::Hasher::new();
    for entry in entries {
        hash.update(&entry.slot.to_be_bytes());
        hash.update(&entry.value_hash.to_be_bytes());
        hash.update(&[entry.source.kind.as_byte()]);
        hash.update(&entry.source.owner_ord.to_be_bytes());
        hash.update(&entry.source.pid.to_be_bytes());
        hash.update(&entry.source.elem.to_be_bytes());
    }
    hash.finalize().to_hex().to_string()
}

/// Absorbs new writes. Exact brute-force scan, no graph — kept small by the
/// caller's freeze threshold, so a flat SIMD-friendly scan is cheap and a
/// provider graph would be pure overhead for few entries.
#[derive(Clone)]
pub(crate) struct HotSegment {
    entries: Vec<SegmentEntry>,
    vectors: Vec<Vec<f32>>,
}

impl HotSegment {
    pub(crate) fn new() -> Self {
        HotSegment {
            entries: Vec::new(),
            vectors: Vec::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Real owned bytes: entry metadata plus every vector's payload
    /// — exact, not a formula estimate.
    pub(crate) fn measured_bytes(&self) -> usize {
        self.entries.len() * std::mem::size_of::<SegmentEntry>()
            + self
                .vectors
                .iter()
                .map(|v| v.len() * std::mem::size_of::<f32>())
                .sum::<usize>()
    }

    pub(crate) fn push(&mut self, entry: SegmentEntry, vector: Vec<f32>) {
        self.entries.push(entry);
        self.vectors.push(vector);
    }

    fn gather(&self, query: &[f32], k: usize, similarity: Similarity) -> Vec<usize> {
        let mut scored: Vec<(f32, usize)> = self
            .vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (crate::score(query, v, similarity), i))
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().take(k).map(|(_, i)| i).collect()
    }
}

impl Default for HotSegment {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable, `usearch`-backed segment. Its graph is either built fresh in
/// memory (`build`) or reopened via `usearch`'s mmap `.view()` (`open`), so
/// a large frozen segment need not be fully resident — the OS page cache
/// bounds RSS, matching the blueprint's memory model (§3.2).
pub(crate) struct FrozenSegment {
    entries: Vec<SegmentEntry>,
    // Read by `manifest()`, used for lifecycle reporting
    // (`ann_lifecycle_in_view`'s `manifestState`/`providerState` shape) and
    // this module's own tests — always populated regardless, so a disk
    // round-trip can validate it.
    manifest: SegmentManifest,
    /// Always a *view*: of `buffer` for an in-memory segment, of
    /// `file` for a persisted one. Building allocates a growable arena (2 ×
    /// 8 MiB minimum, mostly reserved and unused: 16.9 MB for a 5 MB
    /// 3K×768 index); viewing the serialized form holds exactly its bytes.
    /// Declared before `buffer`/`file` so it is dropped first.
    index: usearch::Index,
    /// Serialized index the in-memory view reads (owned, stable address).
    buffer: Option<Box<[u8]>>,
    /// The persisted file the view maps; deleted once superseded.
    file: Option<SegmentFile>,
}

/// A persisted segment file. Removed (with its sidecars) when the last
/// segment holding it drops after [`FrozenSegment::supersede`], so a
/// merged-away or replaced segment never lingers on disk. A
/// segment merely evicted from the cache keeps its file for reuse.
struct SegmentFile {
    path: PathBuf,
    superseded: std::sync::atomic::AtomicBool,
    /// Identity of the file this segment maps; a different file at the same
    /// path is never deleted by this guard.
    identity: Option<(u64, u64)>,
}

fn file_identity(path: &Path) -> Option<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path)
            .ok()
            .map(|meta| (meta.dev(), meta.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

impl SegmentFile {
    fn new(path: &Path) -> Self {
        SegmentFile {
            path: path.to_path_buf(),
            superseded: std::sync::atomic::AtomicBool::new(false),
            identity: file_identity(path),
        }
    }
}

impl Drop for SegmentFile {
    fn drop(&mut self) {
        let doomed = self.superseded.load(std::sync::atomic::Ordering::SeqCst)
            && file_identity(&self.path) == self.identity;
        if doomed {
            // The segment file goes first: a reader of the directory never
            // sees a segment without its sidecars.
            for path in [
                self.path.clone(),
                manifest_path_for(&self.path),
                entries_path_for(&self.path),
            ] {
                if let Err(error) = std::fs::remove_file(&path) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        eprintln!(
                            "rushdb: cannot remove superseded vector segment {}: {error}",
                            path.display()
                        );
                    }
                }
            }
        }
    }
}

/// Graph search breadth fixed at build/open time. Per-query `ef` is honored by
/// over-fetching `max(k, ef)` candidates instead of mutating the shared
/// index's expansion, which raced between concurrent queries (C9,
/// cloud-capacity F11).
const SEARCH_EXPANSION: usize = 128;

fn usearch_options(dims: u32, similarity: Similarity) -> usearch::IndexOptions {
    usearch::IndexOptions {
        dimensions: dims as usize,
        metric: match similarity {
            Similarity::Cosine => usearch::MetricKind::Cos,
            Similarity::Dot => usearch::MetricKind::IP,
            Similarity::Euclidean => usearch::MetricKind::L2sq,
        },
        // f16 halves the graph's resident vectors. Candidates are exactly
        // reranked against the canonical f32 vectors, so quantization can
        // only affect which candidates are gathered, never the returned
        // scores.
        quantization: usearch::ScalarKind::F16,
        connectivity: 0,
        expansion_add: 0,
        expansion_search: SEARCH_EXPANSION,
        multi: false,
    }
}

/// Builds a `usearch` index over `len` vectors, consuming them one at a time:
/// a caller decoding from storage never holds the f32 corpus, only the
/// index under construction.
fn build_index<V: AsRef<[f32]>>(
    len: usize,
    vectors: impl IntoIterator<Item = Result<V, VectorError>>,
    dims: u32,
    similarity: Similarity,
) -> Result<usearch::Index, VectorError> {
    let options = usearch_options(dims, similarity);
    let index = usearch::Index::new(&options).map_err(|e| VectorError::Engine(e.to_string()))?;
    index
        .reserve(len.max(1))
        .map_err(|e| VectorError::Engine(e.to_string()))?;
    let mut added = 0usize;
    for (key, vector) in vectors.into_iter().enumerate() {
        index
            .add(key as u64, vector?.as_ref())
            .map_err(|e| VectorError::Engine(e.to_string()))?;
        added += 1;
    }
    if added != len {
        return Err(VectorError::Engine(format!(
            "segment build: {added} vectors for {len} entries"
        )));
    }
    Ok(index)
}

/// Bytes [`build_index`] holds per vector of `dims` while building, and a
/// built segment's serialized size per vector: the f16 vector, its graph
/// links and the entry. Checked against real builds by
/// `build_bytes_estimate_covers_real_segments`.
pub(crate) fn build_bytes_per_vector(dims: u32) -> usize {
    (dims as usize) * 2 + 320
}

/// One persisted entry in the `.entries` sidecar: slot, owner kind, owner
/// ordinal, pid, element, value hash.
const ENTRY_RECORD_BYTES: usize = 8 + 1 + 8 + 8 + 8 + 8;
const ENTRIES_MAGIC: &[u8; 4] = b"RSE2";

fn encode_entries(entries: &[SegmentEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 8 + entries.len() * ENTRY_RECORD_BYTES);
    out.extend_from_slice(ENTRIES_MAGIC);
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.slot.to_le_bytes());
        out.push(entry.source.kind.as_byte());
        out.extend_from_slice(&entry.source.owner_ord.to_le_bytes());
        out.extend_from_slice(&entry.source.pid.to_le_bytes());
        out.extend_from_slice(&entry.source.elem.to_le_bytes());
        out.extend_from_slice(&entry.value_hash.to_le_bytes());
    }
    out
}

fn decode_entries(bytes: &[u8]) -> Option<Vec<SegmentEntry>> {
    let u64_at = |at: usize| -> Option<u64> {
        Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
    };
    if bytes.get(..4)? != ENTRIES_MAGIC {
        return None;
    }
    let len = usize::try_from(u64_at(4)?).ok()?;
    if bytes.len() != 12 + len.checked_mul(ENTRY_RECORD_BYTES)? {
        return None;
    }
    (0..len)
        .map(|i| {
            let at = 12 + i * ENTRY_RECORD_BYTES;
            Some(SegmentEntry {
                slot: u64_at(at)?,
                source: SourceRef {
                    kind: rushdb_types::ids::OwnerKind::from_byte(bytes[at + 8])?,
                    owner_ord: u64_at(at + 9)?,
                    pid: u64_at(at + 17)?,
                    elem: u64_at(at + 25)?,
                },
                value_hash: u64_at(at + 33)?,
            })
        })
        .collect()
}

impl FrozenSegment {
    /// Real resident bytes: the underlying `usearch` index's own
    /// measured memory usage (near-zero for an mmap-reopened, `.view()`d
    /// index, since the OS page cache backs it rather than the heap) plus
    /// this segment's own entry metadata.
    pub(crate) fn measured_bytes(&self) -> usize {
        self.entries.len() * std::mem::size_of::<SegmentEntry>()
            + self.index.memory_usage()
            + self.buffer.as_ref().map_or(0, |buffer| buffer.len())
    }

    /// Builds a fresh frozen segment from `entries`/`vectors` (same order),
    /// resident in memory only — no disk persistence. Used directly by a
    /// pure in-memory store (no configured segments directory). The built
    /// graph is serialized and viewed, releasing the build arena.
    ///
    /// # Errors
    /// Returns [`VectorError::Engine`] when the `usearch` index cannot be
    /// built or serialized, and whatever a vector of `vectors` yields.
    pub(crate) fn build_in_memory<V: AsRef<[f32]>>(
        entries: Vec<SegmentEntry>,
        vectors: impl IntoIterator<Item = Result<V, VectorError>>,
        dims: u32,
        similarity: Similarity,
    ) -> Result<Self, VectorError> {
        let built = build_index(entries.len(), vectors, dims, similarity)?;
        let mut buffer = vec![0u8; built.serialized_length()].into_boxed_slice();
        built
            .save_to_buffer(&mut buffer)
            .map_err(|e| VectorError::Engine(e.to_string()))?;
        drop(built);
        let index = usearch::Index::new(&usearch_options(dims, similarity))
            .map_err(|e| VectorError::Engine(e.to_string()))?;
        // SAFETY: `buffer` is owned by the returned segment, never mutated,
        // and declared after `index`, so it outlives every use of the view.
        unsafe { index.view_from_buffer(&buffer) }
            .map_err(|e| VectorError::Engine(e.to_string()))?;
        let manifest = SegmentManifest {
            format_version: SEGMENT_FORMAT_VERSION,
            input_fingerprint: segment_fingerprint(&entries),
            entries: entries.len(),
            dims,
        };
        Ok(FrozenSegment {
            entries,
            manifest,
            index,
            buffer: Some(buffer),
            file: None,
        })
    }

    /// Builds a fresh frozen segment from `entries`/`vectors` (same order),
    /// persists it under `path` as `usearch`'s own single-file format plus
    /// two sidecars (`<path>.manifest.json`, `<path>.entries`), and returns a
    /// view of that file (the build arena is released).
    ///
    /// # Errors
    /// Returns [`VectorError::Engine`] when the `usearch` index cannot be
    /// built, persisted or reopened, and whatever a vector of `vectors`
    /// yields.
    pub(crate) fn build<V: AsRef<[f32]>>(
        entries: Vec<SegmentEntry>,
        vectors: impl IntoIterator<Item = Result<V, VectorError>>,
        dims: u32,
        similarity: Similarity,
        path: &Path,
    ) -> Result<Self, VectorError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| VectorError::Engine(format!("segment directory create: {e}")))?;
        }
        let built = build_index(entries.len(), vectors, dims, similarity)?;
        // Written aside and renamed into place, sidecars first: a reader of
        // the directory never sees a segment file without its sidecars.
        let staged = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7().simple()));
        built
            .save(&staged.to_string_lossy())
            .map_err(|e| VectorError::Engine(e.to_string()))?;
        drop(built);
        let manifest = SegmentManifest {
            format_version: SEGMENT_FORMAT_VERSION,
            input_fingerprint: segment_fingerprint(&entries),
            entries: entries.len(),
            dims,
        };
        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|e| VectorError::Engine(format!("segment manifest encode: {e}")))?;
        let staged_manifest = manifest_path_for(&staged);
        let staged_entries = entries_path_for(&staged);
        std::fs::write(&staged_manifest, manifest_bytes)
            .and_then(|()| std::fs::write(&staged_entries, encode_entries(&entries)))
            .map_err(|e| VectorError::Engine(format!("segment sidecar write: {e}")))?;
        std::fs::rename(&staged_entries, entries_path_for(path))
            .and_then(|()| std::fs::rename(&staged_manifest, manifest_path_for(path)))
            .and_then(|()| std::fs::rename(&staged, path))
            .map_err(|e| VectorError::Engine(format!("segment publish: {e}")))?;
        let index = usearch::Index::new(&usearch_options(dims, similarity))
            .map_err(|e| VectorError::Engine(e.to_string()))?;
        index
            .view(&path.to_string_lossy())
            .map_err(|e| VectorError::Engine(e.to_string()))?;
        Ok(FrozenSegment {
            entries,
            manifest,
            index,
            buffer: None,
            file: Some(SegmentFile::new(path)),
        })
    }

    /// Mark this segment's file for deletion once the last holder drops it
    /// (a merge absorbed it, a rebuild replaced it, or it holds no live
    /// entry). No-op for an in-memory segment.
    pub(crate) fn supersede(&self) {
        if let Some(file) = &self.file {
            file.superseded
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Reopens a persisted frozen segment via `usearch`'s mmap `.view()`,
    /// with its entries read back from the `.entries` sidecar and checked
    /// against the manifest's fingerprint. A torn file, missing or
    /// mismatched sidecar, or another dimension is `Ok(None)`: the
    /// canonical vectors remain the source of truth, so the caller simply
    /// decodes what no segment covers.
    ///
    /// # Errors
    /// Returns [`VectorError::Engine`] only for an unexpected I/O failure
    /// reading a sidecar; a missing/corrupt/mismatched one is `Ok(None)`.
    pub(crate) fn open(
        path: &Path,
        dims: u32,
        similarity: Similarity,
    ) -> Result<Option<Self>, VectorError> {
        let read = |path: PathBuf| match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(VectorError::Engine(format!(
                "segment sidecar read {}: {e}",
                path.display()
            ))),
        };
        let (Some(manifest_bytes), Some(entry_bytes)) = (
            read(manifest_path_for(path))?,
            read(entries_path_for(path))?,
        ) else {
            return Ok(None);

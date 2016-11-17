//! Continuous logical-WAL archiving and physical base backups (durability A).
//!
//! The durable state lives in one redb file. To reduce that single-file blast
//! radius to something Postgres-like, an operator can:
//!
//! 1. run a **base backup** (`rushdb serve --backup-base <dir>`) offline — a copy of
//!    the redb file(s) plus a CRC manifest pinned at a commit index; and
//! 2. let the server **archive the WAL** continuously with
//!    `--wal-archive-dir <dir>` — append-only `wal-<from>-<to>.seg` segment
//!    files plus an `archive.state` watermark.
//!
//! Base + archived segments reconstruct any point in the retained window
//! (`rushdb serve --restore-pitr`), so the data file is no longer "one file or
//! nothing" and the log can be shipped off-box.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use rushdb_storage::Store;

/// Manifest written beside a base backup.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BaseManifest {
    /// Manifest format version.
    pub format_version: u32,
    /// Canonical command-batch version encoded by the copied store.
    pub command_version: u32,
    /// Transport-neutral protocol version expected by restore tooling.
    pub protocol_version: u16,
    /// Commit index the base is pinned at.
    pub commit_index: u64,
    /// Backed-up files with sizes and CRCs.
    pub files: Vec<BaseFile>,
}

/// One file in a base backup.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BaseFile {
    /// File name (relative to the base directory).
    pub name: String,
    /// Size in bytes.
    pub bytes: u64,
    /// CRC32 of the file contents.
    pub crc32: u32,
}

const BASE_FORMAT_VERSION: u32 = 1;
const BASE_COMMAND_VERSION: u32 = rushdb_types::command::COMMAND_VERSION;
const BASE_PROTOCOL_VERSION: u16 = rushdb_protocol::PROTOCOL_VERSION;
const MANIFEST_NAME: &str = "manifest.json";
const STATE_NAME: &str = "archive.state";
const BASE_SUBDIR: &str = "base";

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct ArchiveState {
    last_lsn: u64,
}

fn file_size_crc32(path: &Path) -> Result<(u64, u32), String> {
    let file = std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut reader = std::io::BufReader::with_capacity(1024 * 1024, file);
    let mut hasher = Hasher::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut bytes = 0u64;
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes.saturating_add(read as u64);
    }
    Ok((bytes, hasher.finalize()))
}

fn copy_file_streaming(src: &Path, dst: &Path) -> Result<(), String> {
    let mut input = std::fs::File::open(src).map_err(|e| format!("open source: {e}"))?;
    let mut output = std::fs::File::create_new(dst).map_err(|e| format!("create target: {e}"))?;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|e| format!("read source: {e}"))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(|e| format!("write target: {e}"))?;
    }
    output.sync_all().map_err(|e| format!("fsync target: {e}"))
}

fn sync_directory(path: &Path, context: &str) -> Result<(), String> {
    let handle = std::fs::File::open(path)
        .map_err(|error| format!("open {context} directory for fsync: {error}"))?;
    handle
        .sync_all()
        .map_err(|error| format!("fsync {context} directory: {error}"))
}

/// Stream an artifact into a private temporary file and publish it with
/// exclusive-create semantics. The destination is never truncated or
/// replaced, and the completed file plus containing directory are durable
/// before this returns.
///
/// # Errors
/// Returns an error if the target path is invalid, staging or publication
/// fails, the callback fails, or either fsync cannot complete.
pub fn atomic_publish_stream<F>(target: &Path, write: F) -> Result<(), String>
where
    F: FnOnce(&mut std::fs::File) -> Result<(), String>,
{
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("create artifact parent {}: {error}", parent.display()))?;
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid artifact target `{}`", target.display()))?;
    let temporary = parent.join(format!(".{name}.tmp-{}", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut file = std::fs::File::create_new(&temporary)
            .map_err(|error| format!("create artifact staging file: {error}"))?;
        write(&mut file)?;
        file.sync_all()
            .map_err(|error| format!("fsync artifact staging file: {error}"))?;
        // A hard link is an atomic create-if-absent publication on the same
        // filesystem; unlike rename it cannot overwrite a pre-existing target.
        std::fs::hard_link(&temporary, target)
            .map_err(|error| format!("publish artifact `{}`: {error}", target.display()))?;
        std::fs::remove_file(&temporary)
            .map_err(|error| format!("remove artifact staging file: {error}"))?;
        sync_directory(parent, "artifact publication")
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Write `bytes` to `dir/name` atomically (temp + fsync + rename) so a crash
/// never leaves a half-written segment or state file.
fn atomic_write_stream<F>(dir: &Path, name: &str, write: F) -> Result<(), String>
where
    F: FnOnce(&mut std::fs::File) -> Result<(), String>,
{
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let tmp = dir.join(format!(".{name}.tmp"));
    let final_path = dir.join(name);
    let result = (|| {
        let mut file = std::fs::File::create(&tmp).map_err(|e| format!("create temp: {e}"))?;
        write(&mut file)?;
        file.sync_all().map_err(|e| format!("fsync temp: {e}"))?;
        std::fs::rename(&tmp, &final_path).map_err(|e| format!("rename into place: {e}"))?;
        sync_directory(dir, "atomic write")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn atomic_write(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let tmp = dir.join(format!(".{name}.tmp"));
    let final_path = dir.join(name);
    {
        use std::io::Write as _;
        let mut file = std::fs::File::create(&tmp).map_err(|e| format!("create temp: {e}"))?;
        file.write_all(bytes)
            .map_err(|e| format!("write temp: {e}"))?;
        file.sync_all().map_err(|e| format!("fsync temp: {e}"))?;
    }
    std::fs::rename(&tmp, &final_path).map_err(|e| format!("rename into place: {e}"))?;
    sync_directory(dir, "atomic write")?;
    Ok(())
}

/// Last archived LSN from `<dir>/archive.state` (0 when absent).
///
/// # Errors
/// Returns a message when the state file exists but cannot be read or parsed.
pub fn read_watermark(dir: &Path) -> Result<u64, String> {
    let path = dir.join(STATE_NAME);
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(format!("read archive state: {error}")),
    };
    let state: ArchiveState =
        serde_json::from_slice(&raw).map_err(|e| format!("corrupt archive state: {e}"))?;
    Ok(state.last_lsn)
}

/// Archive the WAL range `(last_archived, committed]` into `dir` as one
/// append-only segment, advancing the durable watermark. Returns the new
/// watermark, or `None` when there is nothing new to archive.
///
/// # Errors
/// Returns a message when the export or the durable write fails.
pub fn archive_once(store: &Store, dir: &Path) -> Result<Option<u64>, String> {
    let committed = store.committed_version();
    let last = read_watermark(dir)?;
    if committed <= last {
        return Ok(None);
    }
    let name = format!("wal-{last:020}-{committed:020}.seg");
    atomic_write_stream(dir, &name, |file| {
        store
            .export_wal_segments(file, last.saturating_add(1), committed)
            .map(|_| ())
            .map_err(|e| format!("export wal: {e}"))
    })?;
    let state = serde_json::to_vec(&ArchiveState {
        last_lsn: committed,
    })
    .map_err(|e| format!("encode archive state: {e}"))?;
    atomic_write(dir, STATE_NAME, &state)?;
    Ok(Some(committed))
}

/// Base backups registered in an archive directory, so archive retention
/// knows which WAL a restorable window still needs (SD-7.2).
const BASES_NAME: &str = "bases.json";

/// One base backup the archive can restore from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchivedBase {
    /// Commit index the base is pinned at; replay starts after it.
    pub commit_index: u64,
    /// When the base was taken (unix milliseconds).
    pub taken_at_ms: u64,
}

fn read_bases(dir: &Path) -> Result<Vec<ArchivedBase>, String> {
    match std::fs::read(dir.join(BASES_NAME)) {
        Ok(raw) => serde_json::from_slice(&raw).map_err(|e| format!("corrupt {BASES_NAME}: {e}")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(format!("read {BASES_NAME}: {error}")),
    }
}

/// Register a base backup taken at `commit_index` with the archive in `dir`.
///
/// # Errors
/// Returns a message when the registry cannot be read or rewritten.
pub fn register_base(dir: &Path, commit_index: u64, taken_at_ms: u64) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("create archive dir: {e}"))?;
    let mut bases = read_bases(dir)?;
    bases.push(ArchivedBase {
        commit_index,
        taken_at_ms,
    });
    bases.sort_by_key(|base| (base.taken_at_ms, base.commit_index));
    let encoded = serde_json::to_vec(&bases).map_err(|e| format!("encode bases: {e}"))?;
    atomic_write(dir, BASES_NAME, &encoded)
}

/// What one archive-retention pass removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ArchivePrune {
    /// Segment files deleted.
    pub segments: usize,
    /// Bytes reclaimed.
    pub bytes: u64,
    /// Segments removed by the byte cap although the window still needed
    /// them (the restorable window shrank).
    pub forced: usize,
}

/// Archive retention (SD-7.2): PITR is "any point within the
/// window". The window needs the newest base taken at or before its start
/// (or, failing that, the oldest base inside it) plus every segment after
/// that base. Older segments go once they are past the window by time; the
/// optional byte cap then removes the oldest segments regardless (and says
/// so). Segments are removed oldest first only, so the archive never has a
/// gap.
///
/// # Errors
/// Returns a message when the directory, a segment or the registry cannot be
/// read or a file cannot be removed.
pub fn prune_archive(
    dir: &Path,
    now_ms: u64,
    window: std::time::Duration,
    max_bytes: Option<u64>,
) -> Result<ArchivePrune, String> {
    let window_start = now_ms.saturating_sub(u64::try_from(window.as_millis()).unwrap_or(u64::MAX));
    let mut segments: Vec<(u64, u64, PathBuf, u64, u64)> = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|e| format!("read archive dir: {e}"))? {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some((from, to)) = name
            .strip_prefix("wal-")

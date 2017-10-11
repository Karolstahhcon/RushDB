//! Node backup operations. Context entry points retain caller-owned
//! snapshot, tenant, cancellation, and budget semantics.

use std::sync::Arc;

use rushdb_types::ids::TenantId;

use super::Node;

impl Node {
    /// Export a framed, manifest-verified snapshot stream (W6, D9/P17): a
    /// `RDBWALSNAP` manifest chunk (LSN range pinned to the snapshot commit
    /// index, CRC over all tenants), then one independently checksummed chunk
    /// per tenant's JSONL. Truncated or corrupted artifacts are rejected on
    /// restore. Returns the pinned commit index.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn export_snapshot<W: std::io::Write>(
        &self,
        writer: &mut W,
        scopes: &[String],
    ) -> Result<u64, String> {
        let snap = self.store.read_view().map_err(|e| e.to_string())?;
        let commit_index = snap.commit_index();

        let tenants = self.export_tenants(&snap, scopes)?;

        let mut chunks: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(tenants.len());
        for (tenant, scope) in tenants {
            let (jsonl, _records, _relationships) =
                rushdb_backup::export_tenant_jsonl(&snap, tenant)
                    .map_err(|e| format!("export tenant {tenant:?}: {e}"))?;
            chunks.push((scope.0.to_vec(), jsonl.into_bytes()));
        }
        rushdb_storage::wal::write_snapshot(writer, 0, commit_index, commit_index, &chunks)
            .map_err(|e| format!("write snapshot stream: {e}"))?;
        Ok(commit_index)
    }

    /// Restore a framed snapshot stream written by [`Self::export_snapshot`].
    /// Validates the manifest magic/version, verifies every chunk CRC, and
    /// rejects truncated/corrupted input (greenfield: unframed legacy backups
    /// are not accepted). Returns (records, relationships) materialized.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    pub fn restore_snapshot<R: std::io::Read>(&self, reader: &mut R) -> Result<(u64, u64), String> {
        self.restore_snapshot_progress(reader, |_, _, _| {})
            .map(|(r, rel, _)| (r, rel))
    }

    /// Streaming restore: reads the manifest, then applies each tenant chunk as
    /// it arrives (bounded memory) while reporting per-chunk progress via
    /// `progress(chunk_index, records_so_far, relationships_so_far)`. A
    /// truncated/corrupted stream is rejected before any state is applied.
    /// # Errors
    ///
    /// Returns an error message when the operation fails.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "these casts narrow engine counters and sizes that are bounded by the process address space or clamped before conversion, so truncation cannot occur on supported 64-bit targets"
    )]
    pub fn restore_snapshot_progress<R: std::io::Read>(
        &self,
        reader: &mut R,
        mut progress: impl FnMut(usize, u64, u64),
    ) -> Result<(u64, u64, usize), String> {
        use rushdb_storage::wal::{decode_tenant_chunk, read_framed_chunk, read_snapshot_manifest};
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
        if self.store.committed_version() != 0 {
            return Err("restore target is not fresh; use an empty data directory".into());
        }
        let manifest = read_snapshot_manifest(reader).map_err(|e| format!("restore: {e}"))?;
        // Validate and stage every tenant before mutating the destination. A
        // later corrupt or semantically invalid chunk can therefore never
        // leave a partially restored target. The temporary file keeps the
        // bounded-memory property of the streaming format.
        let validation_store = Arc::new(rushdb_storage::Store::new_in_memory());
        let mut staged = tempfile::NamedTempFile::new()

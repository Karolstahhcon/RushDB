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
            .map_err(|error| format!("restore: create staging file: {error}"))?;
        for index in 0..manifest.tenant_count {
            let Some(raw) = read_framed_chunk(reader).map_err(|e| format!("restore: {e}"))? else {
                return Err(format!(
                    "restore: snapshot truncated: {} of {} tenant chunks present",
                    index, manifest.tenant_count
                ));
            };
            let chunk = decode_tenant_chunk(&raw).map_err(|e| format!("restore: {e}"))?;
            let jsonl = String::from_utf8(chunk.jsonl).map_err(|e| e.to_string())?;
            if chunk.tenant.len() != 32 {
                return Err(format!(
                    "restore: invalid tenant identity length {}",
                    chunk.tenant.len()
                ));
            }
            let scope = rushdb_types::ids::TenantScope(
                chunk
                    .tenant
                    .as_slice()
                    .try_into()
                    .map_err(|_| "restore: invalid tenant identity")?,
            );
            let validation_tenant =
                rushdb_lmpg::tenancy::resolve_or_register(&validation_store, &scope)
                    .map_err(|e| format!("restore: register tenant: {e}"))?;
            let validation_target =
                rushdb_lmpg::Lmpg::new(Arc::clone(&validation_store), validation_tenant);
            rushdb_backup::restore_backup(&validation_target, &jsonl)
                .map_err(|e| format!("restore tenant: {e}"))?;
            staged
                .write_all(&scope.0)
                .and_then(|()| staged.write_all(&(jsonl.len() as u64).to_le_bytes()))
                .and_then(|()| staged.write_all(jsonl.as_bytes()))
                .map_err(|error| format!("restore: stage tenant chunk: {error}"))?;
        }
        staged
            .as_file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|error| format!("restore: rewind staging file: {error}"))?;
        let mut records = 0u64;
        let mut relationships = 0u64;
        for index in 0..manifest.tenant_count {
            let mut tenant_bytes = [0u8; 32];
            staged
                .read_exact(&mut tenant_bytes)
                .and_then(|()| {
                    let mut len = [0u8; 8];
                    staged.read_exact(&mut len)?;
                    let length = usize::try_from(u64::from_le_bytes(len))
                        .map_err(|_| std::io::Error::other("tenant JSONL length overflow"))?;
                    let mut jsonl = vec![0u8; length];
                    staged.read_exact(&mut jsonl)?;
                    let jsonl = String::from_utf8(jsonl)
                        .map_err(|_| std::io::Error::other("tenant JSONL is not UTF-8"))?;
                    Ok((rushdb_types::ids::TenantScope(tenant_bytes), jsonl))
                })
                .map_err(|error| format!("restore: read staged tenant chunk: {error}"))
                .and_then(|(scope, jsonl)| {
                    // A single-tenant restore intentionally maps every source chunk
                    // into its fixed target tenant. Multitenant restores preserve the
                    // framed scope identity (registered in this store's directory)
                    // so chunks cannot silently cross scopes.
                    let target_tenant = if self.multitenant {
                        rushdb_lmpg::tenancy::resolve_or_register(&self.store, &scope)
                            .map_err(|e| format!("restore: register tenant: {e}"))?
                    } else {
                        self.base_tenant
                    };
                    let target = rushdb_lmpg::Lmpg::new(Arc::clone(&self.store), target_tenant);
                    let (r, rel) = rushdb_backup::restore_backup(&target, &jsonl)
                        .map_err(|e| format!("restore tenant: {e}"))?;
                    records += r;
                    relationships += rel;
                    progress(index as usize, records, relationships);
                    Ok(())
                })?;
        }
        Ok((records, relationships, manifest.tenant_count as usize))
    }


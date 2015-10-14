//! Index DDL from statement languages (SD-4.6): Cypher/GQL
//! `CREATE INDEX … FOR … ON …` / `DROP INDEX` / `SHOW INDEXES`, SQL
//! `CREATE INDEX name ON label (prop)`, and Mongo `createIndex` without
//! `dimensions` all reach the same declared-index operations.

use rushdb_language::ddl::{DdlKind, IndexDdl};

use super::{execution_failed, ErrorCode, ProtocolError, ProtocolExecutor, Response};

impl ProtocolExecutor<'_> {
    /// Drop a declared index by name, or a vector index when no declared
    /// index has that name.
    pub(super) fn drop_any_index(
        &self,
        name: &str,
        execution_context: &mut rushdb_engine::ExecutionContext,
    ) -> Result<(), ProtocolError> {
        let declared = rushdb_lmpg::index::by_name(
            execution_context.read_view().as_ref(),
            execution_context.tenant(),
            name,
        )
        .map_err(|e| execution_failed(e.to_string()))?;
        if declared.is_some() {
            self.node.drop_index(name).map_err(execution_failed)
        } else {
            self.node
                .drop_vector_index_with_execution_context(execution_context, name)
                .map(drop)
                .map_err(execution_failed)
        }
    }

    /// Declare an index and answer like a statement mutation.
    pub(super) fn create_declared_index(
        &self,
        name: &str,
        label: &str,
        property: &str,
        kind: DdlKind,
    ) -> Result<Response, ProtocolError> {
        let (kind, consistency) = match kind {
            DdlKind::Ordered => ("ordered", None),
            DdlKind::Fulltext { immediate: true } => ("text", Some("immediate".to_string())),

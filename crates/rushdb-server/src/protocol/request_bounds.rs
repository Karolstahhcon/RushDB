//! A request's own bounds: the deadline, working-memory cap and
//! unindexed-walk limit a client may set. Each only tightens the server's
//! ceiling; the engine handle carries them into every read.

use rushdb_protocol::{ErrorCode, ProtocolError};

/// A request's own bounds: its deadline (the server's request
/// timeout, tightened by the client's), working-set cap and unindexed-walk
/// limit. The engine tightens the server's ceilings with them; it never
/// loosens one.
pub(super) fn request_limits(
    context: &rushdb_protocol::RequestContext,
    server_timeout_ms: u64,
) -> Result<rushdb_lmpg::RequestLimits, ProtocolError> {
    if context.query_memory_max_bytes == Some(0) {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "queryMemoryMaxBytes must be positive",
        ));
    }
    let deadline = effective_deadline_ms(context.deadline_ms, server_timeout_ms)
        .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    Ok(rushdb_lmpg::RequestLimits {
        unindexed_walk_max_owners: context.unindexed_walk_max_owners,
        query_memory_max_bytes: context.query_memory_max_bytes,
        deadline,
    })
}

/// The server's request timeout (`0` = none) bounds every transport; a

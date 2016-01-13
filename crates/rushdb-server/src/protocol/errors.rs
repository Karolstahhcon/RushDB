//! Shared error classification for the canonical protocol executor: engine,
//! storage, and language failures all map to one stable [`ErrorCode`]
//! taxonomy, regardless of which transport (Bolt, gRPC, HTTP, pgwire) a
//! request arrived through.

use rushdb_protocol::{ErrorCode, ProtocolError};

/// Map a [`rushdb_engine::SchedulerError`] (RM-3's single admission owner) to
/// a transport-facing [`ProtocolError`]. Shared by every transport
/// (`grpc.rs`, `pgwire.rs`, `http.rs`) so admission failures surface
/// identically regardless of which one a request arrived through.
#[expect(
    clippy::needless_pass_by_value,
    reason = "used directly as a `map_err` adapter, which supplies the owned error value"
)]
pub(crate) fn scheduler_error_to_protocol_error(
    error: rushdb_engine::SchedulerError,
) -> ProtocolError {
    let message = match error {
        rushdb_engine::SchedulerError::QueueFull => "tenant concurrent request limit exceeded",
        rushdb_engine::SchedulerError::Unavailable => "server is shutting down",
        rushdb_engine::SchedulerError::Panicked => "internal error executing request",
    };
    ProtocolError::new(ErrorCode::Unavailable, message)
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "used directly as a `map_err` adapter, which supplies the owned error value"
)]
pub(crate) fn execution_failed(error: impl ToString) -> ProtocolError {
    let message = error.to_string();
    let code = if message.contains("cancelled") || message.contains("deadline exceeded") {
        ErrorCode::Cancelled
    } else if message.contains("unindexed walk refused") {
        // A fixed property of the query and the data: retrying
        // cannot help; declaring the named index does.
        ErrorCode::InvalidRequest
    } else if message.contains("resource exhausted:")
        || message.contains("storage resource exhausted:")
        || message.contains("execution budget exceeded")
        || message.contains("query working set exceeded")
        || message.contains("row safety limit")
        || message.contains("graph mutation staging exceeds")
    {
        // Budget and admission failures are not generic execution faults.
        // Keep the wire taxonomy stable even when an adapter has not yet
        // exposed its concrete error type.
        ErrorCode::ResourceExhausted
    } else if message.contains("storage unhealthy after lock poisoning")
        || message.contains("tenant_capacity")
    {
        // Residency: every resident tenant is active; retry later.
        ErrorCode::Unavailable
    } else if message.contains("denied by policy") || message.contains("` not authorized") {
        // AuthorizedView's label/property gate; same taxonomy as the
        // pre-execution authorize_search_in_view/authorize_aggregate_in_view.
        // Writes to a label outside `allowedLabels` say "label `X` not
        // authorized".
        ErrorCode::Unauthorized
    } else if message.contains("not found") || message.contains("unknown property") {
        ErrorCode::NotFound
    } else if message.contains("serializable_conflict") || message.contains("transaction_conflict")
    {
        ErrorCode::WriteConflict
    } else if message.contains("conflict")
        || message.contains("already exists")
        || message.contains("label changed")
        || message.contains("reference is stale")
    {
        ErrorCode::Conflict
    } else if message.contains("unsupported") {
        ErrorCode::Unsupported
    } else if is_invalid_request(&message) {
        ErrorCode::InvalidRequest
    } else {
        ErrorCode::ExecutionFailed
    };
    // A fixed payload/operation ceiling requires changing the transaction.
    // Shared-pool and active-transaction pressure may clear after other work
    // finishes, so retain the protocol's retryable default for those cases.
    let fixed_staging_limit = [
        "resource exhausted: transaction staging byte limit",
        "resource exhausted: transaction operation limit",
        "resource exhausted: transaction JSON depth/size limit",
        "resource exhausted: staging size overflow",
        "resource exhausted: staging capacity overflow",
        // Large transactions: a commit past the configured
        // transaction ceiling (`StorageError::ResourceExhausted`, Display
        // = "storage resource exhausted: {0}") — the ETL/bulk-load boundary.
        "storage resource exhausted: expanded mutation batch exceeds",
    ]
    .iter()
    .any(|reason| message.starts_with(reason));
    // SEC-5: do not forward an unclassified execution fault's raw text (paths,
    // key bytes, invariants); log it and return a generic message. Client-actionable
    // codes (invalid request, not found, conflict, limits, cancellation) keep theirs.
    let client_message = if code == ErrorCode::ExecutionFailed {
        tracing::warn!(detail = %message, "suppressing internal execution error detail");
        "internal execution error".to_string()
    } else {
        message
    };
    let error = ProtocolError::new(code, client_message);
    if fixed_staging_limit {
        error.with_retryable(false)
    } else {
        error
    }
}

/// Request validation the engine reports as text: a malformed `where`, a
/// missing required field, an out-of-range option. It describes the caller's
/// input, so the caller sees it. A stored-state fault (`corrupt …`) stays
/// internal however it is phrased.
fn is_invalid_request(message: &str) -> bool {
    !message.contains("corrupt")
        && (message.starts_with("invalid ")
            || message.contains("invalid property name")
            || message.contains("invalid merge key")
            || message.contains(" requires ")
            || message.contains(" require an explicit ")
            || message.contains(" must be "))
}

/// Native query failures carry their class in the error type: an
/// unevaluable predicate is the client's, a query budget or group cap is a
/// fixed property of the query and the data.
#[expect(
    clippy::needless_pass_by_value,
    reason = "used directly as a `map_err` adapter, which supplies the owned error value"
)]
pub(crate) fn query_failed(error: rushdb_query::QueryError) -> ProtocolError {
    use rushdb_query::QueryError;
    let message = error.to_string();
    match error {
        QueryError::InvalidPredicate(_) => ProtocolError::new(ErrorCode::InvalidRequest, message),
        QueryError::Cancelled => ProtocolError::new(ErrorCode::Cancelled, message),
        QueryError::ResourceLimit(_) => {
            ProtocolError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
        }
        QueryError::UnindexedWalk(_) => {
            ProtocolError::new(ErrorCode::InvalidRequest, message).with_retryable(false)
        }
        QueryError::Storage(_) | QueryError::Engine(_) => execution_failed(message),
    }
}


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

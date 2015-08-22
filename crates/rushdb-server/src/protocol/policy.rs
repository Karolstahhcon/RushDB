//! Protocol handler for a tenant's data-access policy (operator only).

use super::{execution_failed, ErrorCode, ProtocolError, ProtocolExecutor, Response};

impl ProtocolExecutor<'_> {
    /// Read or replace the tenant's data-access policy.
    pub(super) fn policy(
        &self,
        operation: rushdb_protocol::PolicyOperation,
        execution_context: &rushdb_engine::ExecutionContext,
    ) -> Result<Response, ProtocolError> {
        match operation {
            rushdb_protocol::PolicyOperation::Get => {
                let active = rushdb_authz::load_active_policy_in_view(
                    execution_context.read_view().as_ref(),
                    execution_context.tenant(),
                )
                .map_err(|error| execution_failed(error.to_string()))?;
                Ok(match active {
                    Some(active) => Response::Policy {

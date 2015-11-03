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
                        policy: Some(policy_to_wire(&active.policy, active.min_count)),
                        enforced: active.mode == rushdb_authz::AuthorizationMode::PolicyEnforced,
                        generation: Some(active.generation),
                    },
                    None => Response::Policy {
                        policy: None,
                        enforced: false,
                        generation: None,
                    },
                })
            }
            rushdb_protocol::PolicyOperation::Put { policy, enforced } => {
                if self.node.maintenance() {
                    return Err(ProtocolError::new(
                        ErrorCode::Unavailable,
                        "maintenance mode: writes are rejected until maintenance is cleared",
                    ));
                }
                let (parsed, min_count) = policy_from_wire(policy)?;
                let mode = if enforced {
                    rushdb_authz::AuthorizationMode::PolicyEnforced

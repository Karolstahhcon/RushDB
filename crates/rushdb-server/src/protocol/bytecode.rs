//! Gremlin bytecode operations become Gremlin statements before dispatch:
//! one translation (`rushdb_language::gremlin_bytecode`), then the same
//! authorization, language gating, execution and atomicity as Gremlin text.

use rushdb_protocol::{
    ErrorCode, Mutation, Operation, ProtocolError, Query, RequestEnvelope, StatementLanguage,
};

fn text(bytecode: &serde_json::Value) -> Result<String, ProtocolError> {
    rushdb_language::gremlin_bytecode::bytecode_to_gremlin(bytecode)
        .map_err(|error| ProtocolError::new(ErrorCode::Unsupported, error.to_string()))

use thiserror::Error;

/// Errors produced while encoding, decoding or parsing canonical types.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TypesError {
    /// The scalar does not match the declared logical type.
    #[error("unencodable value for type {0:?}")]

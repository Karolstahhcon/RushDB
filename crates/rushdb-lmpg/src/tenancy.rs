//! Tenant directory resolution and registration.
//!
//! A scope string is hashed to a [`TenantScope`]; the directory maps it to a
//! 4-byte [`TenantId`] ordinal. Registration is an ordinary commit expanded
//! by LMPG, so it is logged, replicated and replayed deterministically.

use std::sync::Arc;

use rushdb_storage::{CommitOutcome, StorageError, Store};
use rushdb_types::command::Mutation;
use rushdb_types::ids::{TenantId, TenantScope};

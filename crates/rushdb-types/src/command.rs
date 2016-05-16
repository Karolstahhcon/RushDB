use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ids::OwnerKind;
use crate::ids::TenantId;
use crate::model::{Cardinality, Nullability, TypeState};
use crate::occurrence::{EmbeddingBinding, EmbeddingSpaceInfo, OccurrenceRow};

/// Current deterministic command format. Only this version is accepted by
/// the state-machine expander; it is independent of the platform wire protocol.
/// Reset to 1 for the first release (no earlier format was ever shipped);
/// bump it whenever a command type changes.
pub const COMMAND_VERSION: u32 = 1;

/// Deterministic mutation set applied atomically by the replicated state
/// machine. Contains allocated IDs and encoded values; apply never reads a
/// clock, RNG, network or parser (plan documents 05/07).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitBatch {
    /// Command format version used to interpret the batch.
    pub command_version: u32,
    /// Tenant whose keyspace the batch mutates.
    pub tenant: TenantId,
    /// Client-visible transaction identifier.
    pub transaction_id: String,
    /// Caller-chosen idempotency key; replays discover the stored outcome.
    pub commit_key: Uuid,
    /// Commit index the batch is expected to be applied at.
    pub start_index: u64,
    /// Isolation level under which the batch was planned.
    pub isolation: IsolationMode,
    /// Conditions that must hold before any mutation is applied.
    pub preconditions: Vec<Precondition>,
    /// Ordered mutations applied atomically.
    pub mutations: Vec<Mutation>,
}

/// Isolation level used when planning a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationMode {
    /// Observe reads committed before the batch was planned.
    ReadCommitted,
    /// Read a consistent snapshot taken at planning time.
    Snapshot,
    /// Fully serializable execution.
    Serializable,
}

/// A single key condition guarding a batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Precondition {
    /// Key the condition inspects.
    pub key: Vec<u8>,
    /// Condition evaluated against the key.
    pub kind: PreconditionKind,
}

/// The two supported precondition checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreconditionKind {
    /// The key must exist.
    Exists,
    /// The key must be absent.
    Absent,
}

impl Precondition {
    /// Require `key` to exist.
    #[must_use]
    pub fn exists(key: Vec<u8>) -> Self {
        Precondition {
            key,
            kind: PreconditionKind::Exists,
        }
    }

    /// Require `key` to be absent.
    #[must_use]
    pub fn absent(key: Vec<u8>) -> Self {
        Precondition {
            key,
            kind: PreconditionKind::Absent,
        }
    }
}

/// A single deterministic mutation applied by the state machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Mutation {
    /// Publish a tenant policy, using the applying CI as its generation.
    SetSecurityPolicy {
        /// Policy payload to publish.
        policy: crate::security::DataAccessPolicy,
        /// Enforcement strength for the policy.
        mode: crate::security::AuthorizationMode,
    },
    /// Create a record header with the given id, ordinal and label.
    CreateRecordHeader {
        /// Public id of the new record.
        record_id: [u8; 16],
        /// Dense ordinal assigned to the record.
        ordinal: u64,
        /// Primary label of the record.
        label: String,
    },
    /// Delete a record header by id, ordinal and label.
    DeleteRecordHeader {
        /// Dense ordinal of the record.
        ordinal: u64,
        /// Public id of the record.
        record_id: [u8; 16],
        /// Primary label of the record.
        label: String,
    },
    /// Move a record between its single primary-label membership sets while
    /// preserving its stable public id, ordinal, fields, and relationships.
    RetypeRecordHeader {
        /// Public id of the record.
        record_id: [u8; 16],
        /// Dense ordinal of the record.
        ordinal: u64,
        /// Label the record is leaving.
        from_label: String,
        /// Label the record is moving to.
        to_label: String,
    },
    /// Create a relationship header between two records.
    CreateRelationshipHeader {
        /// Public id of the new relationship.
        relationship_id: [u8; 16],
        /// Dense ordinal assigned to the relationship.
        ordinal: u64,
        /// Relationship type.
        rel_type: String,
        /// Ordinal of the source record.
        source_ord: u64,
        /// Ordinal of the target record.
        target_ord: u64,
        /// Public id of the source record.
        source_id: [u8; 16],
        /// Public id of the target record.
        target_id: [u8; 16],
    },
    /// Delete a relationship header by id, ordinal and type.

use serde::{Deserialize, Serialize};

use crate::ids::OwnerKind;
use crate::model::{Cardinality, LogicalType, Nullability, TypeState};

/// `ExplicitNull` is a payload-free occurrence state, never a domain type.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Serialize, Deserialize)]
pub enum OccurrenceValue {
    /// A property explicitly set to null.
    ExplicitNull,
    /// Marker occurrence for an array property with zero elements. Arrays are
    /// stored element-wise, so an empty array has no elements to persist; this
    /// payload-free marker (stored at elem 0) lets reconstruction emit a
    /// genuine `[]` — matching prod, where empty arrays are preserved rather
    /// than collapsed to null.
    EmptyArray,
    /// A stored scalar value of a logical type; its order-preserving payload
    /// is stored inline in the occurrence row.
    Stored {
        /// Logical type of the stored scalar.
        ty: LogicalType,
    },
}

impl OccurrenceValue {
    /// Report whether this occurrence is an explicit null.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, OccurrenceValue::ExplicitNull)
    }
}

/// One property occurrence of a record or relationship. Its identity is
/// `(owner_kind, owner_ord, property_id, elem)`; the canonical row is keyed by
/// exactly that (`keys::occ_key`, DC11).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OccurrenceRow {
    /// Whether the owning row is a record or a relationship.
    pub owner_kind: OwnerKind,
    /// Dense ordinal of the owning row.
    pub owner_ord: u64,
    /// Property id this occurrence belongs to.
    pub property_id: u64,
    /// Property family this occurrence belongs to.
    pub family_id: u64,
    /// Element index within an array property, or `None` for scalars.
    pub elem_ordinal: Option<u64>,
    /// The occurrence's value state. The value hash is not carried: it is a
    /// function of this state and the canonical bytes
    /// ([`occurrence_value_hash`]).
    pub occurrence_value: OccurrenceValue,
}

/// Canonical value hash of an occurrence (null and the empty-array marker
/// share the null hash, as the planner assigns them).
#[must_use]
pub fn occurrence_value_hash(value: OccurrenceValue, payload: &[u8]) -> u64 {
    match value {
        OccurrenceValue::Stored { ty } => {
            crate::hash::value_hash(crate::encoding::logical_tag(ty), payload)
        }
        OccurrenceValue::ExplicitNull | OccurrenceValue::EmptyArray => {
            crate::hash::null_value_hash()
        }
    }
}

/// Durable header for a record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordHeader {
    /// Stable public record id.
    pub record_id: [u8; 16],
    /// Dense ordinal of the record.
    pub ordinal: u64,
    /// Primary label of the record.
    pub label: String,
}

/// Durable header for a relationship.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationshipHeader {
    /// Stable public relationship id.
    pub relationship_id: [u8; 16],
    /// Dense ordinal of the relationship.
    pub ordinal: u64,
    /// Relationship type.
    pub rel_type: String,
    /// Ordinal of the source record.
    pub source_ord: u64,
    /// Ordinal of the target record.
    pub target_ord: u64,
    /// Public id of the source record.
    pub source_id: [u8; 16],
    /// Public id of the target record.
    pub target_id: [u8; 16],
}

/// A normalized property family, scoped to records or relationships.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyFamily {
    /// Dense id of the family.
    pub family_id: u64,
    /// Namespace the property belongs to.
    pub namespace: String,
    /// Whether the family is owned by records or relationships.
    pub owner_scope: OwnerKind,
    /// Normalized name used for lookup.
    pub normalized_name: String,
    /// Human-readable display name.
    pub display_name: String,
}

/// A typed property definition within a family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyDefinition {
    /// Dense id of the property.
    pub property_id: u64,
    /// Family this definition belongs to.
    pub family_id: u64,
    /// Current type resolution state.
    pub type_state: TypeState,
    /// Allowed value cardinality.
    pub cardinality: Cardinality,
    /// Nullability constraint.
    pub nullability: Nullability,
}

/// Immutable vector-space contract bound through stable aliases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Similarity {
    /// Cosine similarity (dot product of L2-normalized vectors).
    #[default]
    Cosine,
    /// Dot product (raw; for best results use normalized vectors).
    Dot,
    /// Euclidean similarity = `1 / (1 + euclidean_distance)` (Neo4j-style).
    Euclidean,
}

impl Similarity {
    /// Lowercase name of the similarity metric.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Similarity::Cosine => "cosine",
            Similarity::Dot => "dot",
            Similarity::Euclidean => "euclidean",
        }
    }
}

/// Immutable metadata for an embedding space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingSpaceInfo {
    /// Dense id of the space.
    pub space_id: u64,
    /// Stable, opaque index name. The engine carries vectors without any notion
    /// of embedding source (managed/external/model) — those are control-plane
    /// concepts. Legacy spaces (persisted without this field) default to empty.
    #[serde(default)]
    pub name: String,
    /// Dimensionality of vectors in the space.
    pub dims: u32,
    /// Similarity metric for the space. Older persisted spaces without this
    /// field default to cosine.
    #[serde(default)]
    pub similarity: Similarity,
}

/// Lifecycle status of an embedding binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BindingStatus {
    /// Binding created but not yet populated.
    Pending,
    /// Binding is populated and eligible for search.
    Ready,
    /// Source value changed, so the binding must be rebuilt.
    Stale,
    /// Binding permanently retired.
    Retired,
}

/// One canonical source -> Space association carrying the exact value hash
/// at bind time. Eligible only while that hash remains current.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmbeddingBinding {
    /// Space this binding targets.

//! Declared secondary indexes: the client chooses what is fast.
//!
//! An index belongs to one record label and one property (by normalized
//! family name, so it covers every typed definition of that property). Its
//! entries are derived rows maintained by the apply-time deriver; the state
//! decides which records they cover, so entries always equal a from-scratch
//! derivation of the base rows, including while a build or drop is running.

use serde::{Deserialize, Serialize};

/// One declared index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDef {
    /// Tenant-local index id (allocated at definition, never reused).
    pub id: u32,
    /// Tenant-unique name.
    pub name: String,
    /// Record label the index covers.
    pub label: String,
    /// Normalized property (family) name.
    pub property: String,
    /// Index kind.
    pub kind: IndexKind,
    /// Build/drop state.
    pub state: IndexState,
}

/// What an index answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IndexKind {
    /// Ordered entries `(value, owner)`: equality, `$in`, ranges, prefix.
    Ordered,
    /// Full-text (BM25) over the property's string values — every string
    /// property of the record when the property is [`TEXT_ALL_PROPERTIES`].
    /// Maintained by the text applier from pending rows.
    Text {
        /// When a search sees a write.
        consistency: TextConsistency,
    },
    /// Structural embeddings: one vector per record of the label, derived
    /// from the graph around it (`FastRP` over every relationship, ignoring
    /// direction), bound into the vector space named after the index.
    /// Maintained by the structural applier from pending rows: a
    /// relationship or record change marks the records within two hops.
    /// Its property is [`TEXT_ALL_PROPERTIES`]: it reads topology, not a
    /// property.
    Structural {
        /// Vector dimensions.
        dimensions: u32,
    },
}

impl IndexKind {
    /// Whether an applier outside the store maintains the index from pending
    /// rows (text and structural), rather than apply-time entries.
    #[must_use]
    pub fn applied(self) -> bool {
        matches!(self, Self::Text { .. } | Self::Structural { .. })
    }
}

/// Property name of a text index covering every string property (and of a
/// structural index, which covers no single property).
pub const TEXT_ALL_PROPERTIES: &str = "*";

/// When a text search sees committed writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TextConsistency {

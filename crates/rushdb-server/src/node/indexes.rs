//! Declared ordered indexes on the node: DDL, listing, and the
//! online builder that advances building and dropping indexes in bounded
//! batches on the maintenance tick.

use std::time::{Duration, Instant};

use rushdb_lmpg::index::{self, IndexStep};
use rushdb_types::index::{IndexDef, IndexKind, IndexState, TextConsistency};

use super::{Node, RequestScope};

/// Records per build or drop commit.
pub(crate) const INDEX_BUILD_BATCH: usize = 1_000;

/// Pending rows an index listing counts at most.
const PENDING_COUNT_LIMIT: usize = 1_000_000;

fn describe(def: &IndexDef) -> serde_json::Value {
    let mut out = serde_json::json!({
        "name": def.name,
        "label": def.label,
        "property": def.property,
        "kind": "ordered",
        "state": def.state.label(),
    });
    match def.kind {
        IndexKind::Ordered => {}
        IndexKind::Text { consistency } => {
            out["kind"] = "text".into();
            out["consistency"] = match consistency {
                TextConsistency::Immediate => "immediate",
                TextConsistency::Eventual => "eventual",
            }
            .into();
        }
        IndexKind::Structural { dimensions } => {
            out["kind"] = "structural".into();
            out["dimensions"] = dimensions.into();
            out["model"] = rushdb_vector::structural::STRUCTURAL_MODEL.into();
        }
    }
    out
}

/// Vector registry row of a structural index, so `query.vector` with
/// `index: <name>` searches it like any vector index.
fn structural_vector_row(def: &IndexDef, dimensions: u32) -> serde_json::Value {
    serde_json::json!({
        "id": def.name,
        "name": def.name,
        "label": def.label,
        "property": def.property,
        "similarity": "cosine",
        "dimensions": dimensions,
        "status": "awaiting_vectors",
        "model": rushdb_vector::structural::STRUCTURAL_MODEL,
        "nameSource": "explicit",
        "owner": "record",
        "generation": uuid::Uuid::now_v7().to_string(),
    })
}

/// Parse a protocol index kind (`ordered` | `text` with a consistency |

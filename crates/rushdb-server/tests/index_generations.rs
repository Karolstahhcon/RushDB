//! 4A — durable index generations. Vector bindings and space metadata must
//! survive reopen and an index must be either correctly served or visibly
//! behind (never falsely ready) after a restart.

use rushdb_protocol::{VectorIndexCandidatesQuery, VectorIndexDefinition, VectorItem};
use rushdb_server::{Node, ServerConfig};

fn disk_node(dir: &std::path::Path) -> Node {
    let mut cfg = ServerConfig {
        data_dir: Some(dir.to_path_buf()),
        ..Default::default()
    };
    Node::bootstrap(&mut cfg).unwrap()
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
)]
fn vector_index_serves_persisted_bindings_after_reopen() {
    let dir = rushdb_storage::TempPath::dir("idx");

    // The binding path uses the derived canonical alias.
    let alias = "emb__BOOK__embedding__cosine__3";
    {
        let node = disk_node(&dir);
        node.scope()
            .create_vector_index(&VectorIndexDefinition {
                name: Some(alias.into()),
                label: "BOOK".into(),
                property: "embedding".into(),
                dimensions: 3,
                similarity: "cosine".into(),
                model: None,
                owner: None,
            })
            .unwrap();
        let empty = node.scope().vector_index_stats(alias).unwrap();
        assert_eq!(empty["status"], "awaiting_vectors");
        // The records must exist for a binding to attach; capture the actual
        // generated record ids.
        let rec_a = node
            .scope()
            .create_record(
                "BOOK",
                &serde_json::json!({ "embedding": [1.0, 0.0, 0.0] }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap()
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        let rec_b = node
            .scope()
            .create_record(
                "BOOK",
                &serde_json::json!({ "embedding": [0.0, 1.0, 0.0] }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap()
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        node.scope()
            .upsert_vector_index(
                alias,
                &[
                    VectorItem {
                        record_id: rec_a.clone(),
                        vector: vec![1.0, 0.0, 0.0],
                        source_hash: None,
                        index_generation: None,
                    },
                    VectorItem {
                        record_id: rec_b.clone(),
                        vector: vec![0.0, 1.0, 0.0],
                        source_hash: None,
                        index_generation: None,
                    },
                ],
            )
            .unwrap();
        // The binding must be searchable on the live node (before reopen).
        let pre = node
            .scope()
            .vector_search(alias, &[1.0, 0.0, 0.0], 1)
            .unwrap();
        assert_eq!(pre.len(), 1, "binding must be served on the live node");
        let ready = node.scope().vector_index_stats(alias).unwrap();
        assert_eq!(ready["status"], "ready");
        assert_eq!(ready["indexedRecords"], 2);
        assert_eq!(ready["sourceTrackedBindings"], 0);
        assert_eq!(ready["externalBindings"], 2);
        assert_eq!(ready["annLifecycle"]["manifestState"], "rebuildRequired");
        let rebuilt = node.scope().revalidate_vector_index(alias).unwrap();
        assert_eq!(rebuilt["annLifecycle"]["manifestState"], "current");
        assert_eq!(rebuilt["annLifecycle"]["providerState"], "memoryCached");
        std::mem::forget(rec_a);
        std::mem::forget(rec_b);
    }

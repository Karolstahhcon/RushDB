use super::*;
use rushdb_types::command::{CommitBatch, IsolationMode, COMMAND_VERSION};
use serde_json::json;

fn on_backends(test: impl Fn(&Node)) {
    let dir = rushdb_storage::TempPath::dir("node-native");
    for persistent in [false, true] {
        let mut cfg = crate::ServerConfig {
            data_dir: persistent.then(|| dir.to_path_buf()),
            max_request_bytes: 64 * 1024 * 1024,
            listen_addr: "127.0.0.1".into(),
            tls_cert: None,
            tls_key: None,
            ..Default::default()
        };
        let node = Node::bootstrap(&mut cfg).unwrap();
        test(&node);
    }
    std::fs::remove_dir_all(dir).unwrap();
}

fn corrupt(node: &Node, key: Vec<u8>) {
    node.store.set_expander(Arc::new(Box::new(move |_, _, txn| {
        txn.put(key.clone(), vec![1]);
        Ok(())
    })));
    let commit_key = uuid::Uuid::now_v7();
    node.store
        .commit(commit_key, |_, _| {
            Ok((
                CommitBatch {
                    command_version: COMMAND_VERSION,
                    tenant: node.scope().tenant(),
                    transaction_id: String::new(),
                    commit_key,
                    start_index: 0,
                    isolation: IsolationMode::ReadCommitted,
                    preconditions: vec![],
                    mutations: vec![],
                },
                (),
            ))
        })
        .unwrap();
    // Restore ordinary deterministic expansion after the fixture-only write.
    drop(node.scope().lmpg());
}

#[test]
fn record_get_and_schema_report_corruption_instead_of_missing_data() {
    on_backends(|node| {
        let record = node
            .scope()
            .import_json_document("Doc", &json!({"name": "before"}))
            .unwrap();
        assert_eq!(
            node.scope()
                .get_record(&hex_simple(&record.record_id))
                .unwrap()["properties"]["name"],
            json!("before")
        );
        let view = node.store.read_view().unwrap();
        let (family_key, _) = view
            .range_prefix(&key(
                node.scope().tenant(),
                &[rushdb_types::keys::P_FAMILY, b""],
            ))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        corrupt(node, family_key);
        assert!(node
            .scope()
            .describe_properties(&json!({}))
            .unwrap_err()
            .contains("corrupt property family"));
        assert!(node
            .scope()
            .resolve_property_name("p0000000000000001")
            .unwrap_err()
            .contains("corrupt property family"));
        let error = node
            .scope()
            .get_record(&hex_simple(&record.record_id))
            .unwrap_err();
        assert!(!error.contains("not found"), "{error}");
        assert_eq!(
            node.scope()
                .record_value_in_view(&view, record.ordinal)
                .unwrap()["properties"]["name"],
            json!("before")
        );
    });
}

#[test]
fn node_retention_consumers_protect_and_release_history_floor() {
    on_backends(|node| {
        assert_eq!(node.protected_retention_floor(), None);
        node.acknowledge_retention_consumer("backup", 12).unwrap();
        node.acknowledge_retention_consumer("replica", 20).unwrap();
        assert_eq!(node.protected_retention_floor(), Some(12));
        node.acknowledge_retention_consumer("backup", 15).unwrap();
        assert_eq!(node.protected_retention_floor(), Some(15));
        assert!(node.release_retention_consumer("replica"));
        assert_eq!(node.protected_retention_floor(), Some(15));
    });
}

#[test]
fn active_read_views_protect_command_metadata_retention_floor() {
    on_backends(|node| {
        let view = node.store.read_view().unwrap();
        assert_eq!(
            node.protected_retention_floor(),
            Some(view.commit_index()),
            "an active snapshot must protect metadata needed for its retry/read state"
        );
        drop(view);
        assert_eq!(node.protected_retention_floor(), None);
    });
}

#[test]
fn retention_consumer_acknowledgements_survive_restart() {
    let dir = rushdb_storage::TempPath::dir("retention");
    let mut cfg = crate::ServerConfig {
        data_dir: Some(dir.to_path_buf()),
        ..Default::default()
    };
    {
        let node = Node::bootstrap(&mut cfg).unwrap();
        node.acknowledge_retention_consumer("backup", 42).unwrap();
        assert_eq!(node.protected_retention_floor(), Some(42));
    }
    let mut reopened_cfg = crate::ServerConfig {
        data_dir: Some(dir.to_path_buf()),
        ..Default::default()
    };
    let reopened = Node::bootstrap(&mut reopened_cfg).unwrap();
    assert_eq!(reopened.protected_retention_floor(), Some(42));
    assert!(reopened.release_retention_consumer("backup"));
    drop(reopened);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn query_response_metadata_uses_the_query_view_after_concurrent_corruption() {
    on_backends(|node| {
        node.scope()
            .import_json_document("Doc", &json!({"name": "before"}))
            .unwrap();
        let (family_key, _) = node
            .store
            .read_view()
            .unwrap()
            .range_prefix(&key(
                node.scope().tenant(),
                &[rushdb_types::keys::P_FAMILY, b""],
            ))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let changed = std::cell::Cell::new(false);
        let (total, records) = node
            .scope()
            .search_with_cancel(&SearchRequest::default(), &|| {
                if !changed.replace(true) {
                    corrupt(node, family_key.clone());
                    node.store.gc(u64::MAX).unwrap();
                }
                false
            })
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(records[0]["properties"]["name"], json!("before"));
        assert_eq!(records[0]["propertyTypes"]["name"], json!("string"));
        assert!(node
            .scope()
            .search_with_cancel(&SearchRequest::default(), &|| false)
            .is_err());
    });
}

#[test]
fn schema_description_propagates_invalid_query_documents() {
    on_backends(|node| {
        node.scope()
            .import_json_document("Doc", &json!({"name": "before"}))
            .unwrap();
        // Unknown nested keys can be interpreted as relationship blocks by
        // the parser. Use an invalid shape of a recognized logical operator.
        let document = json!({"where": {"$and": 1}});
        assert!(rushdb_query::document::parse_document(&document).is_err());
        assert!(node.scope().describe_properties(&document).is_err());
    });
}

#[test]
fn native_record_enumeration_and_bulk_delete_preserve_label_selection() {
    on_backends(|node| {
        let a = node
            .scope()
            .import_json_document("A", &json!({"name": "one"}))
            .unwrap();
        let b = node
            .scope()
            .import_json_document("AB", &json!({"name": "two"}))
            .unwrap();
        let records = node.scope().enumerate_records("A", None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, a.ordinal);
        node.scope()
            .delete_records(&json!({"labels": ["A"]}))
            .unwrap();
        assert!(node.scope().get_record(&hex_simple(&a.record_id)).is_err());
        assert_eq!(
            node.scope().get_record(&hex_simple(&b.record_id)).unwrap()["properties"]["name"],
            json!("two")
        );
        assert_eq!(node.scope().labels().unwrap(), vec!["AB"]);
    });
}

#[test]
fn bulk_delete_streams_in_bounded_pages() {
    // Regression: delete_records materialized every match and committed once
    // per record, so large deletes were unbounded in memory and slower than
    // any client timeout (left partially applied). Deletes now stream in
    // bounded pages with one commit per page: 1100 records take 3 pages
    // (500 + 500 + 100) plus a terminating empty page.
    on_backends(|node| {
        for i in 0..1100 {
            node.scope()
                .import_json_document("Bulk", &json!({"n": i}))
                .unwrap();
        }
        let affected = node
            .scope()
            .delete_records(&json!({"labels": ["Bulk"]}))
            .unwrap();
        assert_eq!(affected, 1100);
        assert!(
            node.scope()
                .enumerate_records("Bulk", None)
                .unwrap()
                .is_empty(),
            "all Bulk records must be deleted"
        );
    });
}

#[test]
fn delete_records_with_cancel_aborts_before_deleting() {
    on_backends(|node| {
        for i in 0..10 {
            node.scope()
                .import_json_document("Cancel", &json!({"n": i}))
                .unwrap();
        }
        // Cancel before the first page: nothing is deleted and the call reports
        // cancellation (E4: a long delete must be abortable, not silent).
        let err = node
            .scope()
            .delete_records_with_cancel(&json!({"labels": ["Cancel"]}), &|| true)
            .unwrap_err();
        assert!(err.contains("operation cancelled"), "got: {err}");
        assert_eq!(
            node.scope()
                .enumerate_records("Cancel", None)
                .unwrap()
                .len(),
            10,
            "a cancelled delete must not remove records"
        );
        // Uncanceled, the same call completes.
        assert_eq!(
            node.scope()
                .delete_records(&json!({"labels": ["Cancel"]}))
                .unwrap(),
            10
        );
    });
}

#[test]
fn relationship_scans_exclude_ordinal_rows_and_propagate_header_corruption() {
    on_backends(|node| {
        let a = node
            .scope()
            .import_json_document("A", &json!({"name": "one"}))
            .unwrap();
        let b = node
            .scope()
            .import_json_document("B", &json!({"name": "two"}))
            .unwrap();
        let edge = node
            .scope()
            .lmpg()
            .create_relationship("LINK", a, b, vec![])
            .unwrap();
        let query = serde_json::from_value(json!({})).unwrap();
        assert_eq!(
            node.scope().scan_relationships(&query).unwrap()["data"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(node
            .scope()
            .relationship_exists(a.ordinal, "LINK", b.ordinal)
            .unwrap());
        corrupt(
            node,
            rushdb_lmpg::reader::keys_ext::rel_row_key(
                node.scope().tenant(),
                &edge.relationship_id,
            ),
        );
        assert!(node
            .scope()
            .scan_relationships(&query)
            .unwrap_err()
            .contains("corrupt relationship header"));
        let command = serde_json::from_value(json!({"sourceLabel": "A", "targetLabel": "B", "sourceType": "LINK", "targetType": "NEW"})).unwrap();
        assert!(node.scope().retype_relations(&command).is_err());
    });
}

#[test]
fn vector_registry_stats_candidates_and_filtered_search_use_native_reads() {
    on_backends(|node| {
        let a = node
            .scope()
            .import_json_document("Doc", &json!({"name": "one"}))
            .unwrap();
        let b = node
            .scope()
            .import_json_document("Doc", &json!({"name": "two"}))
            .unwrap();
        let index = node
            .scope()
            .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
                label: "Doc".into(),
                property: "name".into(),
                dimensions: 2,
                similarity: "cosine".into(),
                name: Some("doc-name-2".into()),
                model: None,
                owner: None,
            })
            .unwrap();
        let alias = index["name"].as_str().unwrap();
        node.scope()
            .upsert_vector_index(
                alias,
                &[
                    rushdb_protocol::VectorItem {
                        record_id: hex_simple(&a.record_id),
                        vector: vec![1., 0.],
                        source_hash: None,
                        index_generation: None,
                    },
                    rushdb_protocol::VectorItem {
                        record_id: hex_simple(&b.record_id),
                        vector: vec![0., 1.],
                        source_hash: None,
                        index_generation: None,
                    },
                ],
            )
            .unwrap();
        let stats = node.scope().vector_index_stats(alias).unwrap();
        assert_eq!(stats["totalRecords"], 2);
        assert_eq!(stats["indexedRecords"], 2);
        let query: rushdb_protocol::VectorQuery = serde_json::from_value(json!({
            "labels": ["Doc"], "property": "name", "vector": [1.0, 0.0], "index": alias,
            "predicate": {"name": "two"}, "limit": 1
        }))
        .unwrap();
        let result = node.scope().search_vectors(&query).unwrap();
        assert_eq!(result["data"][0]["properties"]["name"], "two");
        assert_eq!(
            node.scope().vector_search(alias, &[1., 0.], 1).unwrap()[0].0,
            hex_simple(&a.record_id)
        );
        let view = Arc::new(node.store.read_view().unwrap());
        node.scope()
            .bind_field_vector_external(alias, &hex_simple(&b.record_id), "name", &[1., 0.])
            .unwrap();
        node.scope()
            .lmpg()
            .update_fields(
                b,
                vec![("name".into(), rushdb_lmpg::FieldChange::Set(json!("three")))],
            )
            .unwrap();
        node.store.gc(u64::MAX).unwrap();
        let pinned = node.scope().search_vectors_in_view(&query, &view).unwrap();
        assert_eq!(pinned["data"][0]["properties"]["name"], "two");
        assert_eq!(pinned["data"][0]["score"], 0.0);
        assert!(node.scope().search_vectors(&query).unwrap()["data"]
            .as_array()
            .unwrap()
            .is_empty());
        corrupt(node, node.scope().vector_index_key(alias).into_bytes());
        assert!(node.scope().list_vector_indexes().is_err());
        assert!(node.scope().vector_index_stats(alias).is_err());
        assert!(node
            .scope()
            .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
                label: "Doc".into(),
                property: "name".into(),
                dimensions: 2,
                similarity: "cosine".into(),
                name: Some(alias.into()),
                model: None,
                owner: None,
            })
            .is_err());
    });
}

#[test]
fn short_vector_family_signature_is_an_error_not_a_panic_or_missing_candidate() {
    on_backends(|node| {
        let a = node
            .scope()
            .import_json_document("Doc", &json!({"name": "one"}))
            .unwrap();
        let index = node
            .scope()
            .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
                label: "Doc".into(),
                property: "name".into(),
                dimensions: 2,
                similarity: "cosine".into(),
                name: Some("unused".into()),
                model: None,
                owner: None,
            })
            .unwrap();
        let index_name = index["name"].as_str().unwrap().to_owned();
        corrupt(
            node,
            key(
                node.scope().tenant(),
                &[
                    P_FAMILY_SIG,
                    b"default",
                    &[OwnerKind::Record.as_byte()],
                    b"name",
                ],
            ),
        );
        assert!(node
            .scope()
            .bind_field_vector_external(&index_name, &hex_simple(&a.record_id), "name", &[1., 0.])
            .unwrap_err()
            .contains("corrupt property family signature"));
        let query = serde_json::from_value(
            json!({"label": "Doc", "property": "name", "index": index_name, "skip": 0, "limit": 10}),
        )
        .unwrap();
        assert!(node
            .scope()
            .vector_index_candidates(&query)
            .unwrap_err()
            .contains("corrupt property family signature"));
    });
}

#[test]
fn corrupt_vector_index_metadata_is_not_treated_as_record_index() {
    on_backends(|node| {
        let index = node
            .scope()
            .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
                label: "Doc".into(),
                property: "embedding".into(),
                dimensions: 2,
                similarity: "cosine".into(),
                name: Some("broken".into()),
                model: None,
                owner: None,
            })
            .unwrap();
        let alias = index["name"].as_str().unwrap();
        corrupt(node, node.scope().vector_index_key(alias).into_bytes());
        let query = serde_json::from_value(json!({
            "label": "Doc",
            "property": "embedding",
            "index": alias,
            "skip": 0,
            "limit": 10
        }))
        .unwrap();
        assert!(node
            .scope()
            .vector_index_candidates(&query)
            .unwrap_err()
            .contains("corrupt vector index metadata"));
        assert!(node
            .scope()
            .upsert_vector_index(alias, &[])
            .unwrap_err()
            .contains("corrupt vector index metadata"));

        let missing = serde_json::from_value(json!({
            "label": "Doc",
            "property": "embedding",
            "index": "does-not-exist",
            "skip": 0,
            "limit": 10
        }))
        .unwrap();
        assert_eq!(
            node.scope().vector_index_candidates(&missing).unwrap_err(),
            "vector index not found"
        );
    });
}

#[test]
fn cursor_walk_visits_every_record_once_on_a_pinned_snapshot() {
    // E6: a multi-page cursor walk over one pinned view visits every match
    // exactly once with a stable total, and writes committed mid-walk do
    // not leak into the pages (snapshot isolation). Continuations reuse
    // the creation total instead of rescanning for a count.
    on_backends(|node| {
        for i in 0..1200 {
            node.scope()
                .import_json_document("Walk", &json!({"n": i}))
                .unwrap();
        }
        let search = || rushdb_query::SearchRequest {
            labels: vec!["Walk".to_string()],
            pagination: rushdb_query::Pagination {
                skip: 0,
                limit: Some(500),
            },
            ..Default::default()
        };
        let no_cancel = || false;
        let (total, first, cursor) = node
            .scope()
            .start_cursor(&search(), 60_000, &no_cancel)
            .unwrap();
        assert_eq!(total, 1200);
        assert_eq!(first.len(), 500);
        let cursor = cursor.expect("three pages expected");
        // Mid-walk write: must not affect the pinned walk.
        node.scope()
            .import_json_document("Walk", &json!({"n": 9999}))
            .unwrap();
        let mut ids: std::collections::HashSet<String> = first.iter().map(record_id_of).collect();
        let (total, second, cursor) = node
            .scope()
            .continue_cursor(&cursor, 500, &no_cancel)
            .unwrap();
        assert_eq!(total, 1200, "pinned total survives concurrent writes");
        ids.extend(second.iter().map(record_id_of));
        let cursor = cursor.expect("third page expected");
        let (total, third, cursor) = node
            .scope()
            .continue_cursor(&cursor, 500, &no_cancel)
            .unwrap();
        assert_eq!(total, 1200);
        assert_eq!(third.len(), 200);
        assert!(cursor.is_none(), "walk ends after the last page");
        ids.extend(third.iter().map(record_id_of));
        assert_eq!(ids.len(), 1200, "every record visited exactly once");
        assert!(
            !ids.iter().any(std::string::String::is_empty),
            "all page records carry ids"
        );
    });
}

#[test]
fn cursor_walk_with_filter_keeps_stable_total_across_pages() {
    // E6 filtered path: the known-total continuation skips the exact-count
    // rescan (the reconstruction-heavy half of a filtered page) while
    // reporting the same total on every page.
    on_backends(|node| {
        for i in 0..300 {
            node.scope()
                .import_json_document("Filt", &json!({"n": i}))
                .unwrap();
        }
        let mut search = rushdb_query::SearchRequest {
            labels: vec!["Filt".to_string()],
            pagination: rushdb_query::Pagination {
                skip: 0,
                limit: Some(100),
            },
            ..Default::default()
        };
        search.where_ =
            Some(rushdb_query::document::parse_where(&json!({"n": {"$gte": 100}})).unwrap());
        let no_cancel = || false;
        let (total, first, cursor) = node
            .scope()
            .start_cursor(&search, 60_000, &no_cancel)
            .unwrap();
        assert_eq!(total, 200);
        assert_eq!(first.len(), 100);
        let (total, second, cursor) = node
            .scope()
            .continue_cursor(&cursor.unwrap(), 100, &no_cancel)
            .unwrap();
        assert_eq!((total, second.len()), (200, 100));
        assert!(cursor.is_none());
    });
}

fn record_id_of(record: &serde_json::Value) -> String {
    record
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

#[test]
fn delete_records_respects_caller_limit_and_skip() {
    // Regression: a *limited* delete must delete only that many records. The
    // batched rewrite ignored the caller's window and drained every match,
    // which turned a label-scoped limited probe into a full-label wipe.
    on_backends(|node| {
        for i in 0..10 {
            node.scope()
                .import_json_document("Del", &json!({"n": i}))
                .unwrap();
        }
        let affected = node
            .scope()
            .delete_records(&json!({"labels": ["Del"], "limit": 3}))
            .unwrap();
        assert_eq!(affected, 3, "limit must cap the delete");
        assert_eq!(
            node.scope().enumerate_records("Del", None).unwrap().len(),
            7
        );

        // skip leaves the first two matches in place; limit caps this call.
        let affected = node
            .scope()
            .delete_records(&json!({"labels": ["Del"], "skip": 2, "limit": 2}))
            .unwrap();
        assert_eq!(affected, 2);
        assert_eq!(
            node.scope().enumerate_records("Del", None).unwrap().len(),
            5
        );

        // No limit drains the remainder (legacy behavior).
        let affected = node
            .scope()
            .delete_records(&json!({"labels": ["Del"]}))
            .unwrap();
        assert_eq!(affected, 5);
        assert!(node
            .scope()
            .enumerate_records("Del", None)
            .unwrap()
            .is_empty());

        // limit 0 deletes nothing.
        node.scope()
            .import_json_document("Del", &json!({"n": 1}))
            .unwrap();
        assert_eq!(
            node.scope()
                .delete_records(&json!({"labels": ["Del"], "limit": 0}))
                .unwrap(),
            0
        );
        assert_eq!(
            node.scope().enumerate_records("Del", None).unwrap().len(),
            1
        );
    });
}

#[test]
fn equality_and_in_lookups_resolve_through_the_index_path() {
    // Exercises the equality-posting candidate path end to end: a selective
    // equality on a property must return exactly its matches (and be counted
    // exactly) without depending on the scan order.
    on_backends(|node| {
        for i in 0..200 {
            node.scope()
                .import_json_document(
                    "L",
                    &json!({
                        "extId": format!("id-{i:03}"),
                        "kind": if i == 7 { "needle" } else { "hay" },
                    }),
                )
                .unwrap();
        }
        let search = |predicate| rushdb_query::SearchRequest {
            labels: vec!["L".to_string()],
            where_: Some(rushdb_query::document::parse_where(predicate).unwrap()),
            pagination: rushdb_query::Pagination::default(),
            ..Default::default()
        };
        let by_id = json!({"extId": "id-007"});
        let (total, records) = node.scope().search(&search(&by_id)).unwrap();
        assert_eq!((total, records.len()), (1, 1));
        assert_eq!(records[0]["properties"]["extId"], json!("id-007"));

        let missing_id = json!({"extId": "id-999"});
        let (total, _) = node.scope().search(&search(&missing_id)).unwrap();
        assert_eq!(total, 0, "absent value must match nothing");

        let in_pred = json!({"kind": {"$in": ["needle"]}});
        let (total, _) = node.scope().search(&search(&in_pred)).unwrap();
        assert_eq!(total, 1, "$in index path");
    });
}

#[test]
fn metadata_cache_is_exact_and_invalidated_by_commit() {
    // Commit-index-keyed derived metadata: entries are exact for the current
    // commit and a later commit misses, so discovery loops stay O(1) without
    // ever serving stale labels/counts.
    on_backends(|node| {
        assert!(node.scope().labels().unwrap().is_empty());
        node.scope().import_json_document("A", &json!({})).unwrap();
        assert_eq!(node.scope().labels().unwrap(), vec!["A"]);
        assert_eq!(
            node.scope().label_counts().unwrap(),
            vec![("A".to_string(), 1)]
        );
        node.scope().import_json_document("A", &json!({})).unwrap();
        assert_eq!(
            node.scope().label_counts().unwrap(),
            vec![("A".to_string(), 2)],
            "a commit must invalidate the cached counts"
        );
        node.scope().import_json_document("B", &json!({})).unwrap();
        assert_eq!(node.scope().labels().unwrap(), vec!["A", "B"]);
    });
}

#[test]
fn metadata_and_property_caches_share_one_logical_budget() {
    let mut cfg = crate::ServerConfig::default();
    cfg.resources.logical_cache_bytes = crate::resource_config::ByteSize(64 * 1024);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for index in 0..12 {
        let mut properties = serde_json::Map::new();
        properties.insert(format!("property{index}"), json!(index));
        node.scope()
            .import_json_document(&format!("Label{index}"), &properties.into())
            .unwrap();

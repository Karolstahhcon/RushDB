//! 4B — vector filter-semantics fixtures. Exact search is the recall oracle:
//! a vector query with a predicate must return exactly the records that match
//! the predicate (filtered recall == 1.0), never a top-k-ranked record outside
//! the filter.

use rushdb_authz::{AuthorizationMode, DataAccessPolicy};
use rushdb_lmpg::plan::FieldChange;
use rushdb_protocol::{
    Operation, Query, RequestContext, RequestEnvelope, Response, VectorIndexCandidatesQuery,
    VectorIndexDefinition, VectorItem, VectorQuery,
};
use rushdb_server::{Node, ServerConfig};

fn node() -> Node {
    let mut cfg = ServerConfig::default();
    Node::bootstrap(&mut cfg).unwrap()
}

fn create_vector_record(n: &Node, label: &str, tag: &str, vector: &[f32]) -> String {
    n.scope()
        .create_record(
            label,
            &serde_json::json!({ "tag": tag, "embedding": vector }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap()
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string()
}

fn vector_find(n: &Node, query: &VectorQuery) -> Vec<(String, String)> {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("vector-fixture".into()),
        context: RequestContext::default(),
        operation: Operation::Query {
            query: Query::Vector {
                query: query.clone(),
            },
        },
    };
    let res = rushdb_server::protocol::ProtocolExecutor::new(n, None)
        .execute(envelope)
        .unwrap();
    let Response::Records { records, .. } = res.response else {
        panic!("expected records, got {:?}", res.response);
    };
    records
        .into_iter()
        .filter_map(|r| {
            let obj = r.as_object()?;
            let id = obj.get("id").and_then(|v| v.as_str())?.to_string();
            let tag = obj
                .get("properties")
                .and_then(|p| p.get("tag"))
                .and_then(|v| v.as_str())?
                .to_string();
            Some((id, tag))
        })
        .collect()
}

#[test]
fn policy_masks_vector_record_payloads() {
    let n = node();
    let alias = "emb__DOC__embedding__cosine__2";
    n.scope()
        .create_vector_index(&VectorIndexDefinition {
            name: Some(alias.into()),
            label: "DOC".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            model: None,
            owner: None,
        })
        .unwrap();
    let id = n
        .scope()
        .create_record(
            "DOC",
            &serde_json::json!({"title": "visible", "secret": "hidden"}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    n.scope()
        .upsert_vector_index(
            alias,
            &[VectorItem {
                record_id: id.clone(),
                vector: vec![1.0, 0.0],
                source_hash: None,
                index_generation: None,
            }],
        )
        .unwrap();
    n.scope()
        .security_put_policy(
            &DataAccessPolicy {
                allowed_labels: vec!["DOC".into()],
                visible_properties: std::collections::BTreeMap::from([(
                    "DOC".into(),
                    vec!["title".into()],
                )]),
                ..Default::default()
            },
            AuthorizationMode::PolicyEnforced,
        )
        .unwrap();
    let query = VectorQuery {
        labels: vec!["DOC".into()],
        property: "embedding".into(),
        vector: vec![1.0, 0.0],
        similarity: "cosine".into(),
        index: Some(alias.into()),
        predicate: serde_json::json!({}),
        offset: 0,
        limit: 1,
        ef: None,
        mode: None,
        score_threshold: None,
    };
    let response = rushdb_server::protocol::ProtocolExecutor::new(&n, None)
        .execute(RequestEnvelope {
            protocol_version: 1,
            request_id: Some("policy-vector".into()),
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Vector { query },
            },
        })
        .unwrap()
        .response;
    let Response::Records { records, .. } = response else {
        panic!("expected vector records");
    };
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], id);
    assert_eq!(records[0]["properties"]["title"], "visible");
    assert!(records[0]["properties"].get("secret").is_none());
}

#[test]
fn vector_search_filter_recall_matches_exact_oracle() {
    let n = node();
    let alias = "emb__FRUIT__embedding__cosine__2";
    n.scope()
        .create_vector_index(&VectorIndexDefinition {
            name: Some(alias.into()),
            label: "FRUIT".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            model: None,
            owner: None,
        })
        .unwrap();
    // Records with a `kind` tag + a 2-d vector. All vectors are near [1,0] so
    // the query vector ranks them all; the predicate must be the ONLY filter.
    let apple = create_vector_record(&n, "FRUIT", "apple", &[0.99, 0.05]);
    let banana = create_vector_record(&n, "FRUIT", "banana", &[0.98, 0.1]);
    let cherry = create_vector_record(&n, "FRUIT", "cherry", &[0.97, 0.15]);
    let _ = apple;
    n.scope()
        .upsert_vector_index(
            alias,
            &[
                VectorItem {
                    record_id: apple,
                    vector: vec![0.99, 0.05],
                    source_hash: None,
                    index_generation: None,
                },
                VectorItem {
                    record_id: banana,
                    vector: vec![0.98, 0.1],
                    source_hash: None,
                    index_generation: None,
                },
                VectorItem {
                    record_id: cherry,
                    vector: vec![0.97, 0.15],
                    source_hash: None,
                    index_generation: None,
                },
            ],
        )
        .unwrap();

    // Predicate: only banana. Exact-oracle expectation: exactly banana.
    let hit = vector_find(
        &n,
        &VectorQuery {
            labels: vec!["FRUIT".to_string()],
            property: "embedding".to_string(),
            vector: vec![1.0, 0.0],
            similarity: "cosine".to_string(),
            index: Some(alias.into()),
            predicate: serde_json::json!({ "tag": "banana" }),
            offset: 0,
            limit: 10,
            ef: None,
            mode: None,
            score_threshold: None,
        },
    );
    assert_eq!(hit.len(), 1, "filtered recall must be exact: {hit:?}");
    assert_eq!(hit[0].1, "banana");

    // Predicate: apple or cherry (2 records). The query vector still ranks all
    // three, so a post-rank filter could return banana — but the engine filters
    // BEFORE top-k, so it must return exactly the two matching.
    let hit2 = vector_find(
        &n,
        &VectorQuery {
            labels: vec!["FRUIT".to_string()],
            property: "embedding".to_string(),
            vector: vec![1.0, 0.0],
            similarity: "cosine".to_string(),
            index: Some(alias.into()),
            predicate: serde_json::json!({ "tag": { "$in": ["apple", "cherry"] } }),
            offset: 0,
            limit: 10,
            ef: None,
            mode: None,
            score_threshold: None,
        },
    );
    let mut tags: Vec<&str> = hit2.iter().map(|(_, t)| t.as_str()).collect();
    tags.sort_unstable();
    assert_eq!(
        tags,
        vec!["apple", "cherry"],
        "must not leak banana: {hit2:?}"
    );
}

#[test]
fn ann_ef_query_uses_the_index_and_matches_exact_top_hit() {
    let n = node();
    let alias = "emb__ANNEF__embedding__cosine__2";
    n.scope()
        .create_vector_index(&VectorIndexDefinition {
            name: Some(alias.into()),
            label: "ANNEF".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            model: None,
            owner: None,
        })
        .unwrap();
    let mut items = Vec::new();
    for i in 0..32u16 {
        let angle = f32::from(i) * 0.1;
        let vector = vec![angle.cos(), angle.sin()];
        let record_id = create_vector_record(&n, "ANNEF", &format!("r{i}"), &vector);
        items.push(VectorItem {
            record_id,
            vector,
            source_hash: None,
            index_generation: None,
        });
    }
    n.scope().upsert_vector_index(alias, &items).unwrap();

    let query = |ef: Option<usize>| VectorQuery {
        labels: vec!["ANNEF".into()],
        property: "embedding".into(),
        vector: vec![1.0, 0.0],
        similarity: "cosine".into(),
        index: Some(alias.into()),
        predicate: serde_json::json!({}),
        offset: 0,
        limit: 5,
        ef,
        mode: None,
        score_threshold: None,
    };
    let exact = vector_find(&n, &query(None));
    let approximate = vector_find(&n, &query(Some(32)));
    assert!(!exact.is_empty(), "exact search must return hits");
    assert_eq!(
        approximate, exact,
        "ANN (ef=32) with exact rerank must match the exact top-k on this corpus"
    );
}

#[test]
fn revalidate_reports_ann_build_failure_without_failing_reconciliation() {
    // A tiny derived-cache budget makes the ANN build fail. Revalidation must
    // still succeed (the durable binding-status reconciliation happened) and
    // report the derived-build failure instead of returning an error over a
    // partial mutation.
    let mut cfg = ServerConfig::default();
    cfg.resources.logical_cache_bytes = rushdb_server::resource_config::ByteSize(64);
    let n = Node::bootstrap(&mut cfg).unwrap();
    let alias = "emb__REVAL__embedding__cosine__2";
    n.scope()
        .create_vector_index(&VectorIndexDefinition {
            name: Some(alias.into()),
            label: "REVAL".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            model: None,
            owner: None,
        })
        .unwrap();
    let record_id = create_vector_record(&n, "REVAL", "a", &[1.0, 0.0]);
    n.scope()
        .upsert_vector_index(
            alias,
            &[VectorItem {
                record_id,
                vector: vec![1.0, 0.0],
                source_hash: None,
                index_generation: None,
            }],
        )
        .unwrap();

    let result = n.scope().revalidate_vector_index(alias).unwrap();
    assert!(
        result["annRebuildError"].is_string(),
        "tiny cache must fail the derived ANN build: {result}"
    );
    assert!(result["stale"].is_number());
    assert!(result["retired"].is_number());
}

#[test]
fn zero_cache_budget_refuses_resident_ann_and_searches_exactly() {
    // With the logical cache budget disabled there is no bound on a resident
    // ANN build: the build is refused with an actionable error, and an `ef`
    // search answers exactly instead of failing.
    let mut cfg = ServerConfig::default();
    cfg.resources.logical_cache_bytes = rushdb_server::resource_config::ByteSize(0);
    let n = Node::bootstrap(&mut cfg).unwrap();
    let alias = "emb__ZERO__embedding__cosine__2";
    n.scope()
        .create_vector_index(&VectorIndexDefinition {
            name: Some(alias.into()),
            label: "ZERO".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            model: None,
            owner: None,
        })
        .unwrap();
    let record_id = create_vector_record(&n, "ZERO", "a", &[1.0, 0.0]);
    n.scope()
        .upsert_vector_index(
            alias,
            &[VectorItem {
                record_id,
                vector: vec![1.0, 0.0],
                source_hash: None,
                index_generation: None,
            }],
        )
        .unwrap();
    let result = n.scope().revalidate_vector_index(alias).unwrap();
    let error = result["annRebuildError"]
        .as_str()
        .expect("zero cache budget must fail the ANN build");
    assert!(
        error.contains("logical cache budget"),
        "error must name the missing budget: {error}"
    );
    let query = |ef: Option<usize>| VectorQuery {
        labels: vec!["ZERO".into()],
        property: "embedding".into(),
        vector: vec![1.0, 0.0],
        similarity: "cosine".into(),
        index: Some(alias.into()),
        predicate: serde_json::json!({}),
        offset: 0,
        limit: 1,
        ef,
        mode: None,
        score_threshold: None,
    };
    assert_eq!(
        vector_find(&n, &query(Some(16))),
        vector_find(&n, &query(None))
    );
}

#[test]
fn vector_query_without_labels_searches_the_index_label() {
    let n = node();
    let alias = "emb__NOLABEL__embedding__cosine__2";
    n.scope()
        .create_vector_index(&VectorIndexDefinition {
            name: Some(alias.into()),
            label: "NOLABEL".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            model: None,
            owner: None,
        })
        .unwrap();
    let record_id = create_vector_record(&n, "NOLABEL", "only", &[1.0, 0.0]);
    n.scope()
        .upsert_vector_index(
            alias,
            &[VectorItem {
                record_id,
                vector: vec![1.0, 0.0],
                source_hash: None,
                index_generation: None,
            }],
        )
        .unwrap();
    let hits = vector_find(
        &n,
        &VectorQuery {
            labels: vec![],
            property: "embedding".into(),
            vector: vec![1.0, 0.0],
            similarity: "cosine".into(),
            index: Some(alias.into()),
            predicate: serde_json::json!({}),
            offset: 0,
            limit: 5,
            ef: None,
            mode: None,
            score_threshold: None,
        },
    );
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].1, "only");
}

#[test]
fn stale_index_generation_rejects_late_vector_completion() {
    let n = node();
    let definition = VectorIndexDefinition {
        name: Some("generation-index".into()),
        label: "DOC".into(),
        property: "embedding".into(),
        dimensions: 2,
        similarity: "cosine".into(),
        model: Some("model-v1".into()),
        owner: None,
    };
    let first = n.scope().create_vector_index(&definition).unwrap();
    let old_generation = first["generation"].as_str().unwrap().to_owned();
    n.scope().drop_vector_index("generation-index").unwrap();
    let second = n.scope().create_vector_index(&definition).unwrap();
    let new_generation = second["generation"].as_str().unwrap();
    assert_ne!(old_generation, new_generation);

    let id = create_vector_record(&n, "DOC", "late", &[1.0, 0.0]);
    let current = create_vector_record(&n, "DOC", "current", &[0.0, 1.0]);
    let error = n
        .scope()
        .upsert_vector_index(
            "generation-index",
            &[
                VectorItem {
                    record_id: current,
                    vector: vec![0.0, 1.0],
                    source_hash: None,
                    index_generation: Some(new_generation.to_owned()),
                },
                VectorItem {
                    record_id: id,
                    vector: vec![1.0, 0.0],
                    source_hash: None,
                    index_generation: Some(old_generation),
                },
            ],
        )
        .expect_err("old generation completion");
    assert!(error.contains("stale vector index generation"));
    assert_eq!(
        n.scope().vector_index_stats("generation-index").unwrap()["indexedRecords"],
        0
    );
}

#[test]
fn vector_upsert_rejects_wrong_dimensions_before_binding_any_item() {
    let n = node();
    let alias = "dimension-index";
    n.scope()
        .create_vector_index(&VectorIndexDefinition {
            name: Some(alias.into()),
            label: "DOC".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            model: None,
            owner: None,
        })
        .unwrap();
    let first = create_vector_record(&n, "DOC", "first", &[1.0, 0.0]);
    let second = create_vector_record(&n, "DOC", "second", &[0.0, 1.0]);
    let error = n
        .scope()
        .upsert_vector_index(
            alias,
            &[
                VectorItem {
                    record_id: first,
                    vector: vec![1.0, 0.0],
                    source_hash: None,
                    index_generation: None,
                },
                VectorItem {
                    record_id: second,
                    vector: vec![0.0, 1.0, 0.0],
                    source_hash: None,
                    index_generation: None,
                },
            ],
        )
        .expect_err("wrong-dimension vectors must fail closed");
    assert!(error.contains("does not match index dimension"));
    assert_eq!(
        n.scope().vector_index_stats(alias).unwrap()["indexedRecords"],
        0
    );
}

#[test]
fn explain_vector_reports_access_path_and_candidate_count() {
    use rushdb_protocol::{Describe, Operation, RequestContext, RequestEnvelope, Response};
    let n = node();
    let alias = "emb__FRUIT__embedding__cosine__2";
    n.scope()
        .create_vector_index(&VectorIndexDefinition {
            name: Some(alias.into()),
            label: "FRUIT".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            model: None,
            owner: None,
        })
        .unwrap();
    for (tag, vec) in [("apple", vec![0.99, 0.05]), ("banana", vec![0.98, 0.1])] {
        let id = n
            .scope()
            .create_record(
                "FRUIT",
                &serde_json::json!({ "tag": tag, "embedding": vec }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap()
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        n.scope()
            .upsert_vector_index(
                alias,
                &[VectorItem {
                    record_id: id,
                    vector: vec,
                    source_hash: None,
                    index_generation: None,
                }],
            )
            .unwrap();
    }
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("explain-vector".into()),
        context: RequestContext::default(),
        operation: Operation::Describe {
            describe: Describe::ExplainVector {
                query: VectorQuery {
                    labels: vec!["FRUIT".to_string()],
                    property: "embedding".to_string(),
                    vector: vec![1.0, 0.0],
                    similarity: "cosine".to_string(),
                    index: None,
                    predicate: serde_json::json!({ "tag": "banana" }),
                    offset: 0,
                    limit: 10,
                    ef: None,
                    mode: None,
                    score_threshold: None,
                },
            },
        },
    };
    let res = rushdb_server::protocol::ProtocolExecutor::new(&n, None)
        .execute(envelope)
        .unwrap();
    let Response::ExplainVector { plan } = res.response else {
        panic!("expected explain vector, got {:?}", res.response);
    };
    assert_eq!(plan.access_path, "vector_index");
    assert_eq!(plan.label, "FRUIT");
    assert_eq!(

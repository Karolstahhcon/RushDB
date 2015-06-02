use super::*;
use crate::node::scope::RequestScope;
use rushdb_protocol::{
    Describe, JoinPairsQuery, JoinPredicate, ParameterStyle, Query, RecordSet, RequestContext,
    RequestEnvelope, StatementAccess, StatementLanguage, StatementTransport,
};
use rushdb_storage::{Replicator, Store};
use rushdb_types::command::CommitBatch;
use std::sync::{Arc, Weak};
use std::time::Duration;

fn transaction_request(
    transaction: TransactionOperation,
    deadline_ms: Option<u64>,
    scope: Option<&str>,
) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext {
            scope: scope.map(str::to_string),
            deadline_ms,
            ..Default::default()
        },
        operation: Operation::Transaction { transaction },
    }
}

#[test]
fn rollback_bypasses_phase_admission_when_pool_is_full() {
    let node = Node::bootstrap(&mut crate::ServerConfig::default()).unwrap();
    let transaction_id = node.scope().tx_begin().unwrap();
    let phase_budget = node.store().phase_budget().unwrap();
    let held = phase_budget.try_reserve(phase_budget.available()).unwrap();

    let response = ProtocolExecutor::new(&node, None).execute(transaction_request(
        TransactionOperation::Rollback {
            transaction_id: transaction_id.clone(),
        },
        None,
        None,
    ));

    drop(held);
    assert!(
        response.is_ok(),
        "rollback was denied under pressure: {response:?}"
    );
    assert!(
        node.scope().tx_rollback(&transaction_id).is_err(),
        "rollback must be terminal"
    );
}

struct DelayedAcknowledgement {
    store: Weak<Store>,
}

impl Replicator for DelayedAcknowledgement {
    fn replicate(&self, batch: &CommitBatch, payload: &[u8]) -> Result<(), String> {
        self.store
            .upgrade()
            .ok_or_else(|| "store dropped".to_string())?
            .apply_replicated_batch_with_payload(batch, Some(payload))
            .map_err(|error| error.to_string())?;
        std::thread::sleep(Duration::from_millis(250));
        Ok(())
    }
}

#[test]
fn committed_transaction_survives_late_ack_deadline() {
    let node = Node::bootstrap(&mut crate::ServerConfig::default()).unwrap();
    let transaction_id = node.scope().tx_begin().unwrap();
    node.scope()
        .tx_stage(
            &transaction_id,
            &rushdb_protocol::TransactionMutation::CreateRecord {
                label: "Audit".into(),
                id: None,
                properties: serde_json::json!({"v": 1}),
                options: serde_json::json!({}),
            },
        )
        .unwrap();
    node.store()
        .set_replicator(Some(Arc::new(DelayedAcknowledgement {
            store: Arc::downgrade(&node.store()),
        })));

    let response = ProtocolExecutor::new(&node, None).execute(transaction_request(
        TransactionOperation::Commit {
            transaction_id: transaction_id.clone(),
        },
        Some(100),
        None,
    ));

    let response = response.expect("durable commit must remain reportable");
    assert!(matches!(
        response.response,
        Response::TransactionOutcome {
            status: TransactionStatus::Committed,
            retryable: false,
            ..
        }
    ));
    assert_eq!(node.store().committed_version(), 1);
    assert!(matches!(
        node.scope().try_tx_commit(&transaction_id).unwrap(),
        rushdb_tx::TxOutcome::Committed(1)
    ));
}

#[test]
fn put_record_survives_late_ack_deadline() {
    let node = Node::bootstrap(&mut crate::ServerConfig::default()).unwrap();
    node.store()
        .set_replicator(Some(Arc::new(DelayedAcknowledgement {
            store: Arc::downgrade(&node.store()),
        })));
    let response = ProtocolExecutor::new(&node, None).execute(RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext {
            scope: None,
            deadline_ms: Some(100),
            ..Default::default()
        },
        operation: Operation::Mutate {
            mutation: Mutation::PutRecord {
                mode: PutMode::Create,
                id: None,
                label: Some("Audit".into()),
                properties: serde_json::json!({"v": 1}),
                vectors: Vec::new(),
                options: serde_json::json!({}),
            },
        },
    });
    assert!(matches!(
        response.map(|envelope| envelope.response),
        Ok(Response::Record { .. })
    ));
    assert_eq!(node.store().committed_version(), 1);
}

#[test]
fn oversized_put_record_returns_compact_ack_after_commit() {
    let mut cfg = crate::ServerConfig {
        max_request_bytes: 512,
        ..crate::ServerConfig::default()
    };
    let node = Node::bootstrap(&mut cfg).unwrap();
    let response = ProtocolExecutor::new(&node, None).execute(RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Mutate {
            mutation: Mutation::PutRecord {
                mode: PutMode::Create,
                id: None,
                label: Some("Audit".into()),
                properties: serde_json::json!({"body": "x".repeat(2_048)}),
                vectors: Vec::new(),
                options: serde_json::json!({}),
            },
        },
    });
    assert!(matches!(
        response.map(|envelope| envelope.response),
        Ok(Response::Mutation { affected: 1 })
    ));
    assert_eq!(node.store().committed_version(), 1);
}

#[test]
fn rollback_rejects_a_transaction_from_another_tenant() {
    let node = Node::bootstrap(&mut config(true)).unwrap();
    let transaction_id = RequestScope::new(&node, Some("tenant-a".into()))
        .tx_begin()
        .unwrap();

    let result =
        ProtocolExecutor::new(&node, Some("tenant-b".into())).execute(transaction_request(
            TransactionOperation::Rollback {
                transaction_id: transaction_id.clone(),
            },
            None,
            Some("tenant-b"),
        ));

    assert!(
        result.is_err(),
        "tenant-b must not roll back tenant-a's transaction"
    );
    assert!(RequestScope::new(&node, Some("tenant-a".into()))
        .tx_rollback(&transaction_id)
        .is_ok());
}

fn statement_request(language: StatementLanguage, statement: &str) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: Some(format!("statement-{language:?}")),
        context: RequestContext::default(),
        operation: Operation::Query {
            query: Query::Statement {
                language,
                statement: statement.into(),
                parameters: None,
            },
        },
    }
}

pub(super) fn config(multitenant: bool) -> crate::ServerConfig {
    crate::ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "test-token".into(),
        data_dir: None,
        multitenant,
        capability_secret: None,
        raft: None,
        resources: crate::resource_config::ResourceConfig::default(),
        transactions: crate::resource_config::TransactionConfig::default(),
        languages: rushdb_protocol::LanguageSet::default(),
        max_request_bytes: 64 * 1024 * 1024,
        listen_addr: "127.0.0.1".into(),
        tls_cert: None,
        tls_key: None,
        maintenance: false,
        allow_insecure_listen: false,
        require_index_warmup: false,
        request_timeout_ms: 0,
        max_concurrent_requests_per_tenant: 0,
        explicit_flags: std::collections::BTreeSet::new(),
    }
}

fn join_request(scope: Option<&str>) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: Some("join-1".into()),
        context: RequestContext {
            scope: scope.map(str::to_string),
            deadline_ms: None,
            ..Default::default()
        },
        operation: Operation::Query {
            query: Query::JoinPairs(JoinPairsQuery {
                left: RecordSet {
                    label: "SOURCE".into(),
                    filter: serde_json::json!({}),
                },
                right: RecordSet {
                    label: "TARGET".into(),
                    filter: serde_json::json!({}),
                },
                predicate: JoinPredicate::PropertyOverlap {
                    left_property: "targetId".into(),
                    right_property: "id".into(),
                },
                exclude_same_record: false,
                projection: JoinProjection::Count,
                limit: 100,
            }),
        },
    }
}

#[test]
fn protocol_execution_records_response_work_counters() {
    let mut cfg = config(false);
    cfg.languages.sql = true;
    let node = Node::bootstrap(&mut cfg).unwrap();
    let response = ProtocolExecutor::new(&node, None)
        .execute(statement_request(StatementLanguage::Sql, "SELECT 1"))
        .unwrap();
    assert!(response.server_timing_ms.is_some());
    let rendered = node.metrics_render();
    assert!(rendered.contains("operation=\"query\""));
    assert!(rendered.contains("counter=\"response_rows\""));
    assert!(rendered.contains("counter=\"response_bytes\""));
}

#[test]
fn scoped_schema_and_labels_use_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .import_json_document("Pinned", &serde_json::json!({"v": 1}))
        .unwrap();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Read, None)
        .unwrap();
    // This write must not appear in either scoped describe result because
    // the request context was pinned before it committed.
    node.scope()
        .import_json_document("Pinned", &serde_json::json!({"v": 2}))
        .unwrap();
    let executor = ProtocolExecutor::new(&node, None);
    let query = rushdb_protocol::QueryDocument {
        labels: vec!["Pinned".into()],
        ..Default::default()
    };
    let schema = executor
        .describe(
            Describe::Schema {
                query: query.clone(),
                detail: None,
                skip: None,
                limit: None,
            },
            &|| false,
            &mut context,
        )
        .unwrap();
    let Response::Schema { schema } = schema else {
        panic!("expected schema response")
    };
    assert_eq!(
        schema
            .labels
            .iter()
            .find(|label| label.name == "Pinned")
            .map(|label| label.records_count),
        Some(1)
    );

    let labels = executor
        .describe(Describe::Labels { query }, &|| false, &mut context)
        .unwrap();
    let Response::Labels { labels } = labels else {
        panic!("expected labels response")
    };
    assert_eq!(labels, vec!["Pinned"]);
}

#[test]
fn backup_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .import_json_document("Before", &serde_json::json!({"v": 1}))
        .unwrap();
    let context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Maintenance, None)
        .unwrap();
    node.scope()
        .import_json_document("After", &serde_json::json!({"v": 2}))
        .unwrap();
    let (_, tenants) = node
        .scope()
        .backup_in_view(
            &rushdb_protocol::BackupRequest { scopes: Vec::new() },
            context.read_view(),
        )
        .unwrap();
    assert_eq!(tenants.len(), 1);
    assert_eq!(tenants[0].records, 1);
}

#[test]
fn delete_record_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    let created = node
        .scope()
        .import_json_document("AfterPin", &serde_json::json!({"v": 2}))
        .unwrap();
    let id = crate::node::hex_simple(&created.record_id);
    let mut context = context;
    let error = node
        .scope()
        .delete_record_with_execution_context(&mut context, &id)
        .unwrap_err();
    assert_eq!(error, "record not found");
    assert!(node.scope().get_record(&id).is_ok());
}

#[test]
fn retype_record_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    let created = node
        .scope()
        .import_json_document("AfterPin", &serde_json::json!({"v": 2}))
        .unwrap();
    let id = crate::node::hex_simple(&created.record_id);
    let error = node
        .scope()
        .retype_record_with_execution_context(&mut context, &id, "AfterPin", "Retyped")
        .unwrap_err();
    assert_eq!(error, "record not found");
    assert!(node.scope().get_record(&id).is_ok());
}

#[test]
fn merge_and_replace_use_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let mut merge_context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    let merged = node
        .scope()
        .import_json_document("AfterPin", &serde_json::json!({"v": 2}))
        .unwrap();
    let merge_id = crate::node::hex_simple(&merged.record_id);
    assert_eq!(
        node.scope()
            .merge_record_with_execution_context(
                &mut merge_context,
                &merge_id,
                None,
                &serde_json::json!({"v": 3}),
                &[],
            )
            .unwrap_err(),
        "record not found; merge creates a missing record only when `label` is given"
    );

    let mut replace_context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    let replaced = node
        .scope()
        .import_json_document("AfterPin", &serde_json::json!({"v": 4}))
        .unwrap();
    let replace_id = crate::node::hex_simple(&replaced.record_id);
    assert_eq!(
        node.scope()
            .replace_record_with_execution_context(
                &mut replace_context,
                &replace_id,
                &serde_json::json!({"v": 5}),
                &[],
            )
            .unwrap_err(),
        "record not found"
    );
}

#[test]
fn bulk_delete_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    node.scope()
        .import_json_document("AfterPin", &serde_json::json!({"v": 2}))
        .unwrap();
    let affected = node
        .scope()
        .delete_records_with_execution_context(
            &mut context,
            &serde_json::json!({"labels": ["AfterPin"]}),
            &|| false,
        )
        .unwrap();
    assert_eq!(affected, 0);
}

#[test]
fn delete_property_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    let created = node
        .scope()
        .import_json_document("AfterPin", &serde_json::json!({"late": 2}))
        .unwrap();
    let id = crate::node::hex_simple(&created.record_id);
    let deleted = node
        .scope()
        .delete_property_with_execution_context(&mut context, "late")
        .unwrap();
    assert!(deleted.is_none(), "the pinned view predates `late`");
    assert!(node.scope().get_record(&id).is_ok());
}

#[test]
fn create_record_rejects_an_expired_execution_context() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let mut context = node
        .scope()
        .execution_context(
            rushdb_engine::OperationClass::Write,
            Some(std::time::Duration::ZERO),
        )
        .unwrap();
    let error = node
        .scope()
        .create_record_with_execution_context(
            &mut context,
            None,
            "Expired",
            &serde_json::json!({"v": 1}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap_err();
    assert!(error.contains("deadline"));
}

#[test]
fn ingest_and_index_reject_expired_contexts_before_mutation() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let executor = ProtocolExecutor::new(&node, None);
    let mut ingest_context = node
        .scope()
        .execution_context(
            rushdb_engine::OperationClass::Write,
            Some(std::time::Duration::ZERO),
        )
        .unwrap();
    let ingest_error = executor
        .ingest(
            Ingest::Documents {
                label: Some("Expired".into()),
                documents: serde_json::json!({"v": 1}),
                options: serde_json::json!({}),
            },
            &mut ingest_context,
            &|| false,
        )
        .unwrap_err();
    assert!(ingest_error.message.contains("deadline"));

    let mut index_context = node
        .scope()
        .execution_context(
            rushdb_engine::OperationClass::Write,
            Some(std::time::Duration::ZERO),
        )
        .unwrap();
    let index_error = executor
        .index(
            IndexOperation::CreateVector {
                definition: rushdb_protocol::VectorIndexDefinition {
                    label: "Expired".into(),
                    property: "embedding".into(),
                    dimensions: 2,
                    similarity: "cosine".into(),
                    name: Some("expired-index".into()),
                    model: None,
                    owner: None,
                },
            },
            &mut index_context,
            &|| false,
        )
        .unwrap_err();
    assert!(index_error.message.contains("deadline"));

    let mut ddl_context = node
        .scope()
        .execution_context(
            rushdb_engine::OperationClass::Write,
            Some(std::time::Duration::ZERO),
        )
        .unwrap();
    let ddl_error = executor
        .sql_index_ddl(
            "CREATE VECTOR INDEX expired_sql ON Expired(embedding) DIMENSIONS 2",
            &mut ddl_context,
            &|| false,
        )
        .unwrap_err();
    assert!(ddl_error.message.contains("deadline"));

    let mut purge_context = node
        .scope()
        .execution_context(
            rushdb_engine::OperationClass::Write,
            Some(std::time::Duration::ZERO),
        )
        .unwrap();
    let purge_error = node
        .scope()
        .purge_tenant_with_execution_context(&mut purge_context, &|| false)
        .unwrap_err();
    assert!(purge_error.contains("deadline"));
}

#[test]
fn cancelled_inline_vector_binding_rolls_back_create_only_ingest() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
            label: "Doc".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            name: Some("doc-embedding".into()),
            model: None,
            owner: None,
        })
        .unwrap();
    let checks = std::sync::atomic::AtomicUsize::new(0);
    let error = node
        .scope()
        .ingest_documents_with_cancel(
            Some("Doc"),
            &serde_json::json!({
                "name": "cancelled",
                "$vectors": [{
                    "propertyName": "embedding",
                    "vector": [0.1, 0.2],
                    "similarityFunction": "cosine",
                    "index": "doc-embedding"
                }]
            }),
            &serde_json::json!({}),
            &|| checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 1,
        )
        .unwrap_err();
    assert!(error.contains("rolled back"), "{error}");
    let view = node.store().read_view().unwrap();
    assert!(test_access::label_members(&view, node.scope().tenant(), "Doc").is_empty());
}

#[test]
fn link_by_id_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let source = node
        .scope()
        .create_record(
            "Source",
            &serde_json::json!({}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let source_id = source["id"].as_str().unwrap().to_owned();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    let target = node
        .scope()
        .create_record(
            "Target",
            &serde_json::json!({}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let command = rushdb_protocol::RelationshipIdsMutation {
        relation_type: "LINK".into(),
        direction: rushdb_protocol::Direction::Out,
        source_ids: vec![source_id],
        target_ids: vec![target["id"].as_str().unwrap().to_owned()],
        properties: serde_json::json!({}),
    };
    let error = node
        .scope()
        .link_records_by_id_with_execution_context(&mut context, &command)
        .unwrap_err();
    assert!(error.contains("record not found"));
}

#[test]
fn link_pairs_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let source = node
        .scope()
        .create_record(
            "Source",
            &serde_json::json!({}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    let target = node
        .scope()
        .create_record(
            "Target",
            &serde_json::json!({}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let command = rushdb_protocol::RelationshipPairsMutation {
        relation_type: "LINK".into(),
        direction: rushdb_protocol::Direction::Out,
        pairs: vec![rushdb_protocol::RelationshipPair {
            source_id: source["id"].as_str().unwrap().into(),
            target_id: target["id"].as_str().unwrap().into(),
            relation_type: None,
        }],
        properties: serde_json::json!({}),
    };
    let error = node
        .scope()
        .link_records_pairs_with_execution_context(&mut context, &command)
        .unwrap_err();
    assert!(error.contains("record not found"));
}

#[test]
fn unlink_by_id_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let source = node
        .scope()
        .create_record(
            "Source",
            &serde_json::json!({}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let target = node
        .scope()
        .create_record(
            "Target",
            &serde_json::json!({}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let source_id = source["id"].as_str().unwrap().to_owned();
    let target_id = target["id"].as_str().unwrap().to_owned();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    node.scope()
        .link_records_by_id(&rushdb_protocol::RelationshipIdsMutation {
            relation_type: "LINK".into(),
            direction: rushdb_protocol::Direction::Out,
            source_ids: vec![source_id.clone()],
            target_ids: vec![target_id.clone()],
            properties: serde_json::json!({}),
        })
        .unwrap();
    let result = node
        .scope()
        .unlink_records_by_id_with_execution_context(
            &mut context,
            &rushdb_protocol::UnlinkRecordsMutation {
                relation_types: vec!["LINK".into()],
                direction: Some(rushdb_protocol::Direction::Out),
                source_ids: vec![source_id],
                target_ids: vec![target_id],
            },
        )
        .unwrap();
    assert_eq!(result["deleted"], 0);
}

#[test]
fn retype_relations_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let source = node
        .scope()
        .create_record(
            "Source",
            &serde_json::json!({}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let target = node
        .scope()
        .create_record(
            "Target",
            &serde_json::json!({}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    node.scope()
        .link_records_by_id(&rushdb_protocol::RelationshipIdsMutation {
            relation_type: "OLD".into(),
            direction: rushdb_protocol::Direction::Out,
            source_ids: vec![source["id"].as_str().unwrap().into()],
            target_ids: vec![target["id"].as_str().unwrap().into()],
            properties: serde_json::json!({}),
        })
        .unwrap();
    let result = node
        .scope()
        .retype_relations_with_execution_context(
            &mut context,
            &rushdb_protocol::RetypeRelationsMutation {
                source_label: "Source".into(),
                target_label: "Target".into(),
                source_type: "OLD".into(),
                target_type: "NEW".into(),
                direction: rushdb_protocol::Direction::Out,
            },
        )
        .unwrap();
    assert_eq!(result["retyped"], 0);
}

#[test]
fn unlink_query_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let source = node
        .scope()
        .create_record(
            "Source",
            &serde_json::json!({"k": 1}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let target = node
        .scope()
        .create_record(
            "Target",
            &serde_json::json!({"k": 1}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    node.scope()
        .link_records_by_id(&rushdb_protocol::RelationshipIdsMutation {
            relation_type: "OLD".into(),
            direction: rushdb_protocol::Direction::Out,
            source_ids: vec![source["id"].as_str().unwrap().into()],
            target_ids: vec![target["id"].as_str().unwrap().into()],
            properties: serde_json::json!({}),
        })
        .unwrap();
    let result = node
        .scope()
        .unlink_records_with_execution_context(
            &mut context,
            &rushdb_protocol::RelationshipMutation {
                relation_type: "OLD".into(),
                source: rushdb_protocol::RelationshipSelector {
                    label: "Source".into(),
                    property: Some("k".into()),
                    predicate: serde_json::json!({}),
                },
                target: rushdb_protocol::RelationshipSelector {
                    label: "Target".into(),
                    property: Some("k".into()),
                    predicate: serde_json::json!({}),
                },
                direction: rushdb_protocol::Direction::Out,
                allow_cartesian: false,
                properties: serde_json::json!({}),
            },
        )
        .unwrap();
    assert_eq!(result["deleted"], 0);
}

#[test]
fn link_query_uses_the_request_pinned_view() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Source",
            &serde_json::json!({"k": 1}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let mut context = node
        .scope()
        .execution_context(rushdb_engine::OperationClass::Write, None)
        .unwrap();
    node.scope()
        .create_record(
            "Target",
            &serde_json::json!({"k": 1}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let result = node
        .scope()
        .link_records_with_execution_context(
            &mut context,
            &rushdb_protocol::RelationshipMutation {
                relation_type: "OLD".into(),
                source: rushdb_protocol::RelationshipSelector {
                    label: "Source".into(),
                    property: Some("k".into()),
                    predicate: serde_json::json!({}),
                },
                target: rushdb_protocol::RelationshipSelector {
                    label: "Target".into(),
                    property: Some("k".into()),
                    predicate: serde_json::json!({}),
                },
                direction: rushdb_protocol::Direction::Out,
                allow_cartesian: false,
                properties: serde_json::json!({}),
            },
        )
        .unwrap();
    assert_eq!(result["created"], 0);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
)]
fn operation_kind_labels_every_operation_for_metrics_and_slow_logs() {
    use rushdb_protocol::{
        BackupRequest, Describe, IndexOperation, Ingest, Mutation, Operation, PutMode, Query,
        QueryDocument, RelationshipIdsMutation, RelationshipMutation, RelationshipSelector,
        RetypeRelationsMutation, StatementLanguage, TransactionMutation, TransactionOperation,
        VectorIndexCandidatesQuery, VectorIndexDefinition, VectorQuery,
    };
    let doc = QueryDocument::default();
    let rel = RelationshipMutation {
        relation_type: "LINK".into(),
        source: RelationshipSelector {
            label: "A".into(),
            property: None,
            predicate: serde_json::json!({}),
        },
        target: RelationshipSelector {
            label: "B".into(),
            property: None,
            predicate: serde_json::json!({}),
        },
        direction: rushdb_protocol::Direction::default(),
        allow_cartesian: false,
        properties: serde_json::json!({}),
    };
    let rel_ids = RelationshipIdsMutation {
        relation_type: "LINK".into(),
        direction: rushdb_protocol::Direction::default(),
        source_ids: vec![],
        target_ids: vec![],
        properties: serde_json::json!({}),
    };
    let vector = VectorQuery {
        labels: vec![],
        property: "p".into(),
        vector: vec![],
        similarity: "cosine".into(),
        index: None,
        predicate: serde_json::json!({}),
        offset: 0,
        limit: 10,
        ef: None,
        mode: None,
        score_threshold: None,
    };
    let cases: Vec<(Operation, &str)> = vec![
        (
            Operation::Query {
                query: Query::Statement {
                    language: StatementLanguage::Sql,
                    statement: "SELECT 1".into(),
                    parameters: None,
                },
            },
            "query.statement",
        ),
        (
            Operation::Query {
                query: Query::Records { query: doc.clone() },
            },
            "query.records",
        ),
        (
            Operation::Query {
                query: Query::ExportCsv { query: doc.clone() },
            },
            "query.exportCsv",
        ),
        (
            Operation::Query {
                query: Query::Cursor {
                    cursor_id: "c".into(),
                    limit: 10,
                },
            },
            "query.cursor",
        ),
        (
            Operation::Query {
                query: Query::Record { id: "x".into() },
            },
            "query.record",
        ),
        (
            Operation::Query {
                query: Query::Relationships {
                    query: rushdb_protocol::RelationshipQuery::default(),
                },
            },
            "query.relationships",
        ),
        (
            Operation::Query {
                query: Query::PropertyValues {
                    property: "p".into(),
                    query: doc.clone(),
                    mode: None,
                    limit: None,
                    search: None,
                },
            },
            "query.propertyValues",
        ),
        (
            Operation::Query {
                query: Query::Vector {
                    query: vector.clone(),
                },
            },
            "query.vector",
        ),
        (
            Operation::Query {
                query: Query::VectorIndexCandidates {
                    query: VectorIndexCandidatesQuery {
                        label: "A".into(),
                        property: "p".into(),
                        index: "i".into(),
                        skip: 0,
                        limit: 10,
                    },
                },
            },
            "query.vectorIndexCandidates",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::Statement {
                    language: StatementLanguage::Sql,
                    statement: "SELECT 1".into(),
                    parameters: None,
                },
            },
            "mutate.statement",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::PutRecord {
                    mode: PutMode::Create,
                    id: None,
                    label: Some("A".into()),
                    properties: serde_json::json!({}),
                    vectors: vec![],
                    options: serde_json::json!({}),
                },
            },
            "mutate.putRecord",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::DeleteRecord { id: "x".into() },
            },
            "mutate.deleteRecord",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::RetypeRecord {
                    id: "x".into(),
                    from_label: "A".into(),
                    to_label: "B".into(),
                },
            },
            "mutate.retypeRecord",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::DeleteRecords { query: doc.clone() },
            },
            "mutate.deleteRecords",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::DeleteProperty { name: "p".into() },
            },
            "mutate.deleteProperty",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::LinkRecords {
                    command: rel.clone(),
                },
            },
            "mutate.linkRecords",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::UnlinkRecords { command: rel },
            },
            "mutate.unlinkRecords",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::RetypeRelations {
                    command: RetypeRelationsMutation {
                        source_label: "A".into(),
                        target_label: "B".into(),
                        source_type: "T".into(),
                        target_type: "U".into(),
                        direction: rushdb_protocol::Direction::default(),
                    },
                },
            },
            "mutate.retypeRelations",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::LinkRecordsById { command: rel_ids },
            },
            "mutate.linkRecordsById",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::LinkRecordsPairs {
                    command: rushdb_protocol::RelationshipPairsMutation {
                        relation_type: "LINK".into(),
                        direction: rushdb_protocol::Direction::default(),
                        pairs: vec![],
                        properties: serde_json::json!({}),
                    },
                },
            },
            "mutate.linkRecordsPairs",
        ),
        (
            Operation::Mutate {
                mutation: Mutation::UnlinkRecordsById {
                    command: rushdb_protocol::UnlinkRecordsMutation {
                        relation_types: vec![],
                        direction: None,
                        source_ids: vec![],
                        target_ids: vec![],
                    },
                },
            },
            "mutate.unlinkRecordsById",
        ),
        (
            Operation::Describe {
                describe: Describe::Schema {
                    query: doc.clone(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
            "describe.schema",
        ),
        (
            Operation::Describe {
                describe: Describe::Labels { query: doc.clone() },
            },
            "describe.labels",
        ),
        (
            Operation::Describe {
                describe: Describe::Properties {
                    query: doc.clone(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
            "describe.properties",
        ),
        (
            Operation::Describe {
                describe: Describe::QueryLanguages,
            },
            "describe.queryLanguages",
        ),
        (
            Operation::Describe {
                describe: Describe::Capabilities,
            },
            "describe.capabilities",
        ),
        (
            Operation::Describe {
                describe: Describe::BuildInfo,
            },
            "describe.buildInfo",
        ),
        (
            Operation::Describe {
                describe: Describe::Explain { query: doc },
            },
            "describe.explain",
        ),
        (
            Operation::Describe {
                describe: Describe::ExplainVector { query: vector },
            },
            "describe.explainVector",
        ),
        (
            Operation::Describe {
                describe: Describe::VectorIndexes,
            },
            "describe.vectorIndexes",
        ),
        (
            Operation::Describe {
                describe: Describe::VectorIndex { name: "i".into() },
            },
            "describe.vectorIndex",
        ),
        (
            Operation::Ingest {
                ingest: Ingest::Documents {
                    label: None,
                    documents: serde_json::json!([]),
                    options: serde_json::json!({}),
                },
            },
            "ingest.documents",
        ),
        (
            Operation::Ingest {
                ingest: Ingest::Csv {
                    label: "A".into(),
                    data: String::new(),
                    parse_config: rushdb_protocol::CsvParseConfig::default(),
                    options: serde_json::json!({}),
                    vectors: Vec::new(),
                },
            },
            "ingest.csv",
        ),
        (
            Operation::Index {
                index: IndexOperation::CreateVector {
                    definition: VectorIndexDefinition {
                        label: "A".into(),
                        property: "p".into(),
                        dimensions: 4,
                        similarity: "cosine".into(),
                        name: None,
                        model: None,
                        owner: None,
                    },
                },
            },
            "index.createVector",
        ),
        (
            Operation::Index {
                index: IndexOperation::DropVector { name: "i".into() },
            },
            "index.dropVector",
        ),
        (
            Operation::Index {
                index: IndexOperation::UpsertVectors {
                    name: "i".into(),
                    items: vec![],
                },
            },
            "index.upsertVectors",
        ),
        (
            Operation::Index {
                index: IndexOperation::RevalidateVector { name: "i".into() },
            },
            "index.revalidateVector",
        ),
        (
            Operation::Transaction {
                transaction: TransactionOperation::Begin {
                    ttl_ms: None,
                    autocommit: false,
                },
            },
            "transaction.begin",
        ),
        (
            Operation::Transaction {
                transaction: TransactionOperation::Stage {
                    transaction_id: "t".into(),
                    mutation: TransactionMutation::CreateRecord {
                        label: "A".into(),
                        id: None,
                        properties: serde_json::json!({}),
                        options: serde_json::json!({}),
                    },
                },
            },
            "transaction.stage",
        ),
        (
            Operation::Transaction {
                transaction: TransactionOperation::Commit {
                    transaction_id: "t".into(),
                },
            },
            "transaction.commit",
        ),
        (
            Operation::Transaction {
                transaction: TransactionOperation::Rollback {
                    transaction_id: "t".into(),
                },
            },
            "transaction.rollback",
        ),
        (
            Operation::Index {
                index: IndexOperation::Create {
                    definition: rushdb_protocol::IndexDefinition {
                        name: "i".into(),
                        label: "A".into(),
                        property: "p".into(),
                        kind: "ordered".into(),
                        consistency: None,
                        dimensions: None,
                    },
                },
            },
            "index.create",
        ),
        (
            Operation::Index {
                index: IndexOperation::Drop { name: "i".into() },
            },
            "index.drop",
        ),
        (
            Operation::Describe {
                describe: Describe::Indexes,
            },
            "describe.indexes",
        ),
        (Operation::PurgeTenant, "purgeTenant"),
        (
            Operation::Backup {
                request: BackupRequest { scopes: vec![] },
            },
            "backup",
        ),
        (
            Operation::Query {
                query: Query::Hybrid {
                    query: rushdb_protocol::HybridQuery {
                        labels: vec![],
                        predicate: serde_json::json!({}),
                        vector: None,
                        text: None,
                        offset: 0,
                        limit: 10,
                        rrf_k: 60.0,
                        fusion: rushdb_protocol::Fusion::Rrf,
                    },
                },
            },
            "query.hybrid",
        ),
    ];
    // JoinPairs and transaction Stage/Rollback go through shared helpers to
    // avoid rebuilding their nested documents here.
    assert_eq!(
        operation_kind(&join_request(None).operation),
        "query.joinPairs"
    );
    for (operation, expected) in cases {
        assert_eq!(operation_kind(&operation), expected, "{operation:?}");
        // Labels aggregate by area prefix on dashboards.
        assert!(
            expected.contains('.') || !expected.contains('/'),
            "label {expected} must be area.detail"
        );
    }
}

#[test]
fn successful_envelopes_echo_server_timing() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let envelope = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Describe {
                describe: Describe::BuildInfo,
            },
        })
        .unwrap();
    assert!(
        envelope.server_timing_ms.is_some(),
        "successful envelopes must echo server timing"
    );
}

#[test]
fn cancelled_mutation_returns_the_cancelled_error_code() {
    // E4: a long mutation must be abortable and surface `Cancelled`, not a
    // generic execution failure or a silent partial success.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Cancel",
            &serde_json::json!({ "n": 1 }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let envelope = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Mutate {
            mutation: Mutation::DeleteRecords {
                query: rushdb_protocol::QueryDocument {
                    labels: vec!["Cancel".into()],
                    ..Default::default()
                },
            },
        },
    };
    let error = ProtocolExecutor::new(&node, None)
        .execute_cancellable(envelope, &|| true)
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Cancelled);
    assert!(error.message.contains("cancelled"));
}

#[test]
fn request_deadline_is_rejected_before_dispatch() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let error = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                deadline_ms: Some(0),
                ..RequestContext::default()
            },
            operation: Operation::Describe {
                describe: Describe::BuildInfo,
            },
        })
        .expect_err("expired request deadline");
    assert_eq!(error.code, ErrorCode::Cancelled);
    assert!(error.message.contains("deadline"));
}

#[test]
fn generic_join_query_counts_distinct_pairs() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "SOURCE",
            &serde_json::json!({ "targetId": ["t1", "t1"] }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(
            "TARGET",
            &serde_json::json!({ "id": "t1" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();

    let response = ProtocolExecutor::new(&node, None)
        .execute(join_request(None))
        .unwrap();
    assert_eq!(response.response, Response::Count { value: 1 });
}

#[test]
fn generic_join_query_honors_mid_scan_cancellation() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for i in 0..8u64 {
        node.scope()
            .create_record(
                "SOURCE",
                &serde_json::json!({ "targetId": format!("t{i}") }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
        node.scope()
            .create_record(
                "TARGET",
                &serde_json::json!({ "id": format!("t{i}") }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let calls = std::cell::Cell::new(0u32);
    let error = ProtocolExecutor::new(&node, None)
        .execute_cancellable(join_request(None), &|| {
            let call = calls.get();
            calls.set(call + 1);
            call > 0
        })
        .expect_err("join must stop when cancellation arrives during the scan");
    assert_eq!(error.code, ErrorCode::Cancelled);
    assert!(error.message.contains("cancelled"));
    assert!(calls.get() > 1);
}

#[test]
fn protocol_executes_all_statement_languages() {
    use rushdb_lmpg::plan::FieldChange;

    let mut cfg = config(false);
    cfg.languages = rushdb_protocol::LanguageSet {
        sql: true,
        gql: true,
        cypher: true,
        gremlin: true,
        sql_pgq: true,
        mongodb: true,
    };
    let node = Node::bootstrap(&mut cfg).unwrap();
    let graph = node.scope().lmpg();
    let alice = graph
        .create_record(
            "Person",
            None,
            vec![("name".into(), FieldChange::Set(serde_json::json!("Alice")))],
        )
        .unwrap();
    let bob = graph
        .create_record(
            "Person",
            None,
            vec![("name".into(), FieldChange::Set(serde_json::json!("Bob")))],
        )
        .unwrap();
    graph
        .create_relationship("KNOWS", alice, bob, vec![])
        .unwrap();

    let cases = [
            (
                StatementLanguage::Sql,
                "SELECT name FROM Person ORDER BY name",
            ),
            (
                StatementLanguage::Cypher,
                "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
            ),
            (
                StatementLanguage::Gql,
                "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
            ),
            (
                StatementLanguage::Gremlin,
                "g.V().hasLabel('Person').values('name').order()",
            ),
            (
                StatementLanguage::SqlPgq,
                "SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (n.name AS name)) AS g ORDER BY name",
            ),
        ];

    for (language, statement) in cases {
        let response = ProtocolExecutor::new(&node, None)
            .execute(statement_request(language, statement))
            .unwrap();
        let Response::Rows { rows, total } = response.response else {
            panic!("{language:?} returned a non-row response")
        };
        assert_eq!(total, 2, "{language:?}: {rows:?}");
        let encoded = serde_json::to_string(&rows).unwrap();
        assert!(encoded.contains("Alice"), "{language:?}: {encoded}");
        assert!(encoded.contains("Bob"), "{language:?}: {encoded}");
    }
}

#[test]
fn protocol_binds_relational_sql_parameters() {
    let mut cfg = config(false);
    cfg.languages = rushdb_protocol::LanguageSet {
        sql: true,
        gql: true,
        cypher: true,
        gremlin: true,
        sql_pgq: true,
        mongodb: true,
    };
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "NOTE",
            &serde_json::json!({ "name": "alpha", "position": 7, "active": true }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();

    let mut request = statement_request(
        StatementLanguage::Sql,
        "SELECT name FROM NOTE WHERE position > $1 AND active = $2",
    );
    let Operation::Query {
        query: Query::Statement { parameters, .. },
    } = &mut request.operation
    else {
        unreachable!()
    };
    *parameters = Some(serde_json::json!([6, true]));

    let response = ProtocolExecutor::new(&node, None).execute(request).unwrap();
    assert_eq!(
        response.response,
        Response::Rows {
            rows: vec![serde_json::json!({ "name": "alpha" })],
            total: 1,
        }
    );
}

#[test]
fn protocol_advertises_honest_query_language_profiles() {
    let mut cfg = config(false);
    cfg.languages = rushdb_protocol::LanguageSet {
        sql: true,
        gql: true,
        cypher: true,
        gremlin: true,
        sql_pgq: true,
        mongodb: true,
    };
    let node = Node::bootstrap(&mut cfg).unwrap();
    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: Some("language-capabilities".into()),
            context: RequestContext::default(),
            operation: Operation::Describe {
                describe: Describe::QueryLanguages,
            },
        })
        .unwrap();
    let Response::QueryLanguages { languages } = response.response else {
        panic!("query language discovery returned the wrong response")
    };

    assert_eq!(languages.len(), 6);
    assert!(languages.iter().all(|capability| {
        capability.access
            == match capability.language {
                StatementLanguage::Gql
                | StatementLanguage::Cypher
                | StatementLanguage::Gremlin
                | StatementLanguage::Mongo
                | StatementLanguage::Sql => StatementAccess::ReadWrite,
                StatementLanguage::SqlPgq => StatementAccess::ReadOnly,
            }
    }));
    let sql = languages
        .iter()
        .find(|capability| capability.language == StatementLanguage::Sql)
        .unwrap();
    assert_eq!(
        sql.parameter_styles,
        vec![ParameterStyle::Positional, ParameterStyle::Named]
    );
    assert!(sql.transports.contains(&StatementTransport::Pgwire));
    assert_eq!(
        sql.mutation_transports,
        vec![StatementTransport::ProtocolJson, StatementTransport::Pgwire]
    );
    assert!(languages
        .iter()
        .filter(|capability| capability.language != StatementLanguage::Sql)
        .all(|capability| capability.transports == vec![StatementTransport::ProtocolJson]));
}

#[test]
fn graph_language_writes_require_the_write_authorized_mutation_operation() {
    let mut cfg = config(false);
    cfg.languages = rushdb_protocol::LanguageSet {
        sql: true,
        gql: true,
        cypher: true,
        gremlin: true,
        sql_pgq: true,
        mongodb: true,
    };
    let node = Node::bootstrap(&mut cfg).unwrap();

    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: Some("cypher-create".into()),
            context: RequestContext::default(),
            operation: Operation::Mutate {
                mutation: Mutation::Statement {
                    language: StatementLanguage::Cypher,
                    statement: "CREATE (n:Person {name: 'Ada'}) RETURN n.name AS name".into(),
                    parameters: None,
                },
            },
        })
        .unwrap();
    let Response::StatementMutation {
        rows,
        affected,
        commit_index,
        ..
    } = response.response
    else {
        panic!("statement mutation returned the wrong response")
    };
    assert_eq!(rows, vec![serde_json::json!({"name": "Ada"})]);
    assert!(affected > 0);
    assert!(commit_index > 0);

    let read_path = ProtocolExecutor::new(&node, None).execute(statement_request(
        StatementLanguage::Cypher,
        "CREATE (:Person {name: 'Eve'})",
    ));
    assert!(read_path.is_err());
}

#[test]
fn sql_dml_uses_only_the_write_authorized_mutation_operation() {
    let mut cfg = config(false);
    cfg.languages.sql = true;
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Person",
            &serde_json::json!({"name": "Ada", "score": 1}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();

    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: Some("sql-update".into()),
            context: RequestContext::default(),
            operation: Operation::Mutate {
                mutation: Mutation::Statement {
                    language: StatementLanguage::Sql,
                    statement: "UPDATE person SET score = score + $increment".into(),
                    parameters: Some(serde_json::json!({"increment": 2})),
                },
            },
        })
        .unwrap();
    let Response::StatementMutation { affected, .. } = response.response else {
        panic!("SQL mutation returned the wrong response")
    };
    assert_eq!(affected, 1);

    let rows = ProtocolExecutor::new(&node, None)
        .execute(statement_request(
            StatementLanguage::Sql,
            "SELECT score FROM person",
        ))
        .unwrap();
    assert_eq!(
        rows.response,
        Response::Rows {
            rows: vec![serde_json::json!({"score": 3})],
            total: 1,
        }
    );

    let read_path = ProtocolExecutor::new(&node, None).execute(statement_request(
        StatementLanguage::Sql,
        "DELETE FROM person",
    ));
    assert!(read_path.is_err());
}

#[test]
fn record_query_plain_projection_filters_and_returns_selected_fields() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "STARSHIP",
            &serde_json::json!({
                "name": "Millennium Falcon",
                "pilot_character_ids": ["character_han_solo", "character_chewbacca"]
            }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(

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
            "STARSHIP",
            &serde_json::json!({
                "name": "X-wing",
                "pilot_character_ids": ["character_rey"]
            }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();

    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: Some("plain-projection-1".into()),
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Records {
                    query: QueryDocument {
                        labels: vec!["STARSHIP".into()],
                        predicate: serde_json::json!({
                            "name": { "$contains": "falcon" }
                        }),
                        select: Some(serde_json::json!({
                            "starship": "$record.name",
                            "pilot_character_ids": "$record.pilot_character_ids"
                        })),
                        limit: Some(100),
                        ..QueryDocument::default()
                    },
                },
            },
        })
        .unwrap();

    assert_eq!(
        response.response,
        Response::Rows {
            total: 1,
            rows: vec![serde_json::json!({
                "starship": "Millennium Falcon",
                "pilot_character_ids": ["character_han_solo", "character_chewbacca"]
            })]
        }
    );
}

#[test]
fn typed_transaction_protocol_commits_atomically() {
    use rushdb_protocol::{TransactionMutation, TransactionOperation, TransactionStatus};

    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let request = |transaction| RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Transaction { transaction },
    };

    let begun = ProtocolExecutor::new(&node, None)
        .execute(request(TransactionOperation::Begin {
            ttl_ms: None,
            autocommit: false,
        }))
        .unwrap();
    let Response::TransactionBegun { transaction_id } = begun.response else {
        panic!("expected transaction id")
    };
    ProtocolExecutor::new(&node, None)
        .execute(request(TransactionOperation::Stage {
            transaction_id: transaction_id.clone(),
            mutation: TransactionMutation::CreateRecord {
                label: "Atomic".into(),
                id: None,
                properties: serde_json::json!({ "value": 42 }),
                options: serde_json::json!({}),
            },
        }))
        .unwrap();
    let committed = ProtocolExecutor::new(&node, None)
        .execute(request(TransactionOperation::Commit {
            transaction_id: transaction_id.clone(),
        }))
        .unwrap();
    assert!(matches!(
        committed.response,
        Response::TransactionOutcome {
            status: TransactionStatus::Committed,
            commit_index: Some(_),
            retryable: false,
            ..
        }
    ));
    assert_eq!(node.scope().labels().unwrap(), vec!["Atomic"]);

    // Commit retries discover the same terminal outcome.
    let retried = ProtocolExecutor::new(&node, None)
        .execute(request(TransactionOperation::Commit { transaction_id }))
        .unwrap();
    assert!(matches!(
        retried.response,
        Response::TransactionOutcome {
            status: TransactionStatus::Committed,
            retryable: false,
            ..
        }
    ));
}

#[test]
fn expired_transaction_request_can_still_roll_back() {
    use rushdb_protocol::{TransactionOperation, TransactionStatus};

    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let begun = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Transaction {
                transaction: TransactionOperation::Begin {
                    ttl_ms: None,
                    autocommit: false,
                },
            },
        })
        .unwrap();
    let Response::TransactionBegun { transaction_id } = begun.response else {
        panic!("expected transaction id")
    };
    let rolled_back = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: None,
                deadline_ms: Some(0),
                ..Default::default()
            },
            operation: Operation::Transaction {
                transaction: TransactionOperation::Rollback { transaction_id },
            },
        })
        .unwrap();
    assert!(matches!(
        rolled_back.response,
        Response::TransactionOutcome {
            status: TransactionStatus::RolledBack,
            ..
        }
    ));
}

#[test]
fn policy_denies_transaction_read_your_writes_for_hidden_label() {
    use rushdb_protocol::{TransactionMutation, TransactionOperation};

    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let hidden = node
        .scope()
        .create_record(
            "Denied",
            &serde_json::json!({"secret": "hidden"}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let hidden_id = hidden["id"].as_str().unwrap().to_owned();
    node.scope()
        .security_put_policy(
            &rushdb_authz::DataAccessPolicy {
                allowed_labels: vec!["Allowed".into()],
                ..Default::default()
            },
            rushdb_authz::AuthorizationMode::PolicyEnforced,
        )
        .unwrap();
    let executor = ProtocolExecutor::new(&node, None);
    let request = |transaction| RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Transaction { transaction },
    };
    let Response::TransactionBegun { transaction_id } = executor
        .execute(request(TransactionOperation::Begin {
            ttl_ms: None,
            autocommit: false,
        }))
        .unwrap()
        .response
    else {
        panic!("expected transaction id")
    };
    let denied = executor
        .execute(request(TransactionOperation::Stage {
            transaction_id: transaction_id.clone(),
            mutation: TransactionMutation::GetRecord {
                record: rushdb_protocol::RecordReference { id: hidden_id },
            },
        }))
        .unwrap_err();
    assert_eq!(denied.code, ErrorCode::Unauthorized);
    executor
        .execute(request(TransactionOperation::Rollback { transaction_id }))
        .unwrap();
}

/// Cross-service transaction lifecycle: the tx id is exchanged between
/// "services" (separate executor calls); a read inside the tx sees staged
/// writes (read-your-writes), and a second service can roll back the whole
/// transaction while the writes stay invisible outside it.
#[test]
fn cross_service_transaction_reads_your_writes_and_rolls_back_atomically() {
    use rushdb_protocol::{TransactionMutation, TransactionOperation, TransactionStatus};

    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let executor = ProtocolExecutor::new(&node, None);
    let request = |transaction| RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Transaction { transaction },
    };
    let stage = |id, mutation| {
        executor
            .execute(request(TransactionOperation::Stage {
                transaction_id: id,
                mutation,
            }))
            .unwrap()
    };

    // Service A begins and creates a record.
    let Response::TransactionBegun { transaction_id: tx } = executor
        .execute(request(TransactionOperation::Begin {
            ttl_ms: None,
            autocommit: false,
        }))
        .unwrap()
        .response
    else {
        panic!("expected tx id")
    };

    // Service B (same tx id, different request) stages a record with a
    // pinned id so Service C can reference it before commit.
    stage(
        tx.clone(),
        TransactionMutation::CreateRecord {
            label: "Person".into(),
            id: Some("00000000-0000-0000-0000-000000000001".into()),
            properties: serde_json::json!({ "name": "Ada" }),
            options: serde_json::json!({}),
        },
    );
    // Service C reads the FIRST staged record back inside the tx (RYW).
    let Response::TransactionGet { record, .. } = stage(
        tx.clone(),
        TransactionMutation::GetRecord {
            record: rushdb_protocol::RecordReference {
                id: "00000000-0000-0000-0000-000000000001".into(),
            },
        },
    )
    .response
    else {
        panic!("expected transaction get")
    };
    assert_eq!(
        record.get("__label").and_then(|v| v.as_str()).unwrap_or(""),
        "Person",
        "read inside the tx must see staged writes"
    );
    assert_eq!(
        record
            .get("properties")
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        "Ada"
    );

    // Nothing is visible outside the tx yet (snapshot isolation).
    assert!(node.scope().labels().unwrap().is_empty());

    // Service D rolls back; the writes never appear.
    let rb = executor
        .execute(request(TransactionOperation::Rollback {
            transaction_id: tx,
        }))
        .unwrap();
    assert!(matches!(
        rb.response,
        Response::TransactionOutcome {
            status: TransactionStatus::RolledBack,
            ..
        }
    ));
    assert!(
        node.scope().labels().unwrap().is_empty(),
        "rollback discards all writes"
    );
}

/// Two transactions writing the same record conflict: the loser aborts and
/// retries cleanly on a fresh base.
#[test]
fn concurrent_transactions_on_same_record_conflict_and_retry() {
    use rushdb_protocol::{TransactionMutation, TransactionOperation, TransactionStatus};

    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let executor = ProtocolExecutor::new(&node, None);
    let request = |transaction| RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Transaction { transaction },
    };
    let stage = |id, mutation| {
        executor
            .execute(request(TransactionOperation::Stage {
                transaction_id: id,
                mutation,
            }))
            .unwrap()
    };
    let begin = |executor: &ProtocolExecutor<'_>| -> String {
        let Response::TransactionBegun { transaction_id } = executor
            .execute(request(TransactionOperation::Begin {
                ttl_ms: None,
                autocommit: false,
            }))
            .unwrap()
            .response
        else {
            panic!("expected tx id")
        };
        transaction_id
    };

    let tx_a = begin(&executor);
    stage(
        tx_a.clone(),
        TransactionMutation::CreateRecord {
            label: "Doc".into(),
            id: None,
            properties: serde_json::json!({ "v": 1 }),
            options: serde_json::json!({}),
        },
    );
    // A concurrent transaction plans the same counter (shared record domain).
    let tx_b = begin(&executor);
    stage(
        tx_b.clone(),
        TransactionMutation::CreateRecord {
            label: "Doc".into(),
            id: None,
            properties: serde_json::json!({ "v": 2 }),
            options: serde_json::json!({}),
        },
    );

    let Response::TransactionOutcome {
        status: status_a, ..
    } = executor
        .execute(request(TransactionOperation::Commit {
            transaction_id: tx_a,
        }))
        .unwrap()
        .response
    else {
        panic!("expected outcome")
    };
    assert_eq!(status_a, TransactionStatus::Committed);

    let Response::TransactionOutcome {
        status: status_b,
        reason,
        retryable,
        ..
    } = executor
        .execute(request(TransactionOperation::Commit {
            transaction_id: tx_b,
        }))
        .unwrap()
        .response
    else {
        panic!("expected outcome")
    };
    assert_eq!(
        status_b,
        TransactionStatus::Aborted,
        "second writer to the same counter must conflict"
    );
    assert!(
        retryable,
        "a first-committer-wins conflict must be retryable"
    );
    assert!(
        reason
            .as_deref()
            .unwrap_or("")
            .contains("transaction_conflict"),
        "conflict reason expected, got {reason:?}"
    );
}

/// A graph-language mutation staged inside a transaction commits with it
/// and rolls back with it.
#[test]
fn statement_language_mutation_stages_commits_and_rolls_back() {
    use rushdb_protocol::{
        StatementLanguage, TransactionMutation, TransactionOperation, TransactionStatus,
    };

    let mut cfg = config(false);
    cfg.languages.cypher = true;
    let node = Node::bootstrap(&mut cfg).unwrap();
    let executor = ProtocolExecutor::new(&node, None);
    let request = |transaction| RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Transaction { transaction },
    };
    let begin = || -> String {
        let Response::TransactionBegun { transaction_id } = executor
            .execute(request(TransactionOperation::Begin {
                ttl_ms: None,
                autocommit: false,
            }))
            .unwrap()
            .response
        else {
            panic!("expected tx id")
        };
        transaction_id
    };
    let stage_statement = |id: &str, statement: &str| {
        executor
            .execute(request(TransactionOperation::Stage {
                transaction_id: id.to_string(),
                mutation: TransactionMutation::Statement {
                    language: StatementLanguage::Cypher,
                    statement: statement.to_string(),
                    parameters: None,
                },
            }))
            .unwrap()
    };

    // Commit path: the staged Cypher write is durable after commit.
    let commit_tx = begin();
    stage_statement(&commit_tx, "CREATE (n:Person {name: 'Ada'})");
    let committed = executor
        .execute(request(TransactionOperation::Commit {
            transaction_id: commit_tx,
        }))
        .unwrap();
    assert!(matches!(
        committed.response,
        Response::TransactionOutcome {
            status: TransactionStatus::Committed,
            ..
        }
    ));
    let lmpg = node.scope().lmpg();
    let snap = lmpg.store.read_view().unwrap();
    assert_eq!(
        test_access::label_members(&snap, lmpg.tenant, "Person").len(),
        1,
        "staged Cypher create is durable after tx commit"
    );

    // Rollback path: the staged Cypher write is discarded.
    let rollback_tx = begin();
    stage_statement(&rollback_tx, "CREATE (n:Person {name: 'Bob'})");
    let rb = executor
        .execute(request(TransactionOperation::Rollback {
            transaction_id: rollback_tx,
        }))
        .unwrap();
    assert!(matches!(
        rb.response,
        Response::TransactionOutcome {
            status: TransactionStatus::RolledBack,
            ..
        }
    ));
    let lmpg2 = node.scope().lmpg();
    let snap2 = lmpg2.store.read_view().unwrap();
    assert_eq!(
        test_access::label_members(&snap2, lmpg2.tenant, "Person").len(),
        1,
        "rolled-back Cypher write is discarded (only Ada remains)"
    );
}

/// A statement-staged transaction conservatively conflicts with any commit
/// that lands after BEGIN, because statement planning does not capture its
/// predicate read-set (phantom prevention).
#[test]
fn statement_transaction_conflicts_with_a_concurrent_commit() {
    use rushdb_protocol::{
        StatementLanguage, TransactionMutation, TransactionOperation, TransactionStatus,
    };
    let mut cfg = config(false);
    cfg.languages.cypher = true;
    let node = Node::bootstrap(&mut cfg).unwrap();
    let executor = ProtocolExecutor::new(&node, None);
    let request = |transaction| RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Transaction { transaction },
    };

    let Response::TransactionBegun { transaction_id } = executor
        .execute(request(TransactionOperation::Begin {
            ttl_ms: None,
            autocommit: false,
        }))
        .unwrap()
        .response
    else {
        panic!("expected tx id")
    };
    executor
        .execute(request(TransactionOperation::Stage {
            transaction_id: transaction_id.clone(),
            mutation: TransactionMutation::Statement {
                language: StatementLanguage::Cypher,
                statement: "CREATE (n:Person {name: 'Ada'})".into(),
                parameters: None,
            },
        }))
        .unwrap();
    // A concurrent autocommit after BEGIN must force the staged statement tx to
    // abort rather than commit a plan based on stale state.
    node.scope()
        .create_record(
            "Concurrent",
            &serde_json::json!({"n": 1}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let outcome = executor
        .execute(request(TransactionOperation::Commit { transaction_id }))
        .unwrap();
    assert!(
        matches!(
            outcome.response,
            Response::TransactionOutcome {
                status: TransactionStatus::Aborted,
                ..
            }
        ),
        "expected a conflict abort, got {:?}",
        outcome.response
    );
}

/// Declare a whole-record text index on `label` and build it.
fn text_index(node: &Node, name: &str, label: &str, consistency: &str) {
    node.scope()
        .create_index(&rushdb_protocol::IndexDefinition {
            name: name.into(),
            label: label.into(),
            property: "*".into(),
            kind: "text".into(),
            consistency: Some(consistency.into()),
            dimensions: None,
        })
        .unwrap();
    while node.step_index_builds(std::time::Duration::from_secs(5)) > 0 {}
}

/// Lexical full-text search is reachable through the protocol and resolves
/// hits to public record ids.
#[test]
fn text_query_returns_lexical_hits_with_record_ids() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    text_index(&node, "docs", "Doc", "immediate");
    node.scope()
        .create_record(
            "Doc",
            &serde_json::json!({"body": "the quick brown fox"}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();

    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Text {
                    index: "docs".into(),
                    query: "quick".into(),
                    limit: 10,
                    all_terms: false,
                    phrase: false,
                },
            },
        })
        .unwrap()
        .response;
    let Response::TextHits { rows, total, .. } = response else {
        panic!("expected rows, got {response:?}");
    };
    assert_eq!(total, 1);
    assert!(rows[0]["id"].is_string(), "{rows:?}");
    assert!(rows[0]["score"].is_number(), "{rows:?}");
}

/// A hybrid query fuses the vector and lexical legs by reciprocal-rank
/// fusion, but only after the shared `where` predicate restricts both legs
/// to the same candidate set. `Doc(tag="drop")` is a perfect vector *and*
/// lexical match (identical embedding and text to the kept records) and
/// must still never appear: the predicate is resolved once, before either
/// leg ranks anything, not applied as a post-filter that top-k could leak
/// around.
#[test]
fn hybrid_query_fuses_legs_inside_one_predicate_resolved_candidate_set() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
            label: "Doc".into(),
            property: "embedding".into(),
            similarity: "cosine".into(),
            dimensions: 2,
            name: Some("doc-embedding".into()),
            model: None,
            owner: None,
        })
        .unwrap();
    text_index(&node, "docs", "Doc", "immediate");

    let make = |tag: &str, text: &str, vector: [f32; 2]| {
        node.scope()
            .create_record(
                "Doc",
                &serde_json::json!({ "tag": tag, "body": text }),
                &[serde_json::json!({
                    "propertyName": "embedding",
                    "vector": vector,
                    "index": "doc-embedding",
                })],
                &serde_json::json!({}),
            )
            .unwrap()
    };
    // Strong match on both legs, inside the predicate.
    make("keep", "the quick brown fox", [1.0, 0.0]);
    // Weak match on both legs, inside the predicate.
    make("keep", "a lazy dog sleeps", [0.0, 1.0]);
    // Strong match on both legs, OUTSIDE the predicate — must never surface.
    make("drop", "the quick brown fox", [1.0, 0.0]);

    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Hybrid {
                    query: rushdb_protocol::HybridQuery {
                        labels: vec!["Doc".into()],
                        predicate: serde_json::json!({"tag": "keep"}),
                        vector: Some(rushdb_protocol::HybridVectorLeg {
                            property: "embedding".into(),
                            vector: vec![1.0, 0.0],
                            similarity: "cosine".into(),
                            index: Some("doc-embedding".into()),
                            ef: None,
                            mode: None,
                            score_threshold: None,
                            weight: 1.0,
                            prefetch_limit: None,
                        }),
                        text: Some(rushdb_protocol::HybridTextLeg {
                            index: "docs".into(),
                            query: "quick".into(),
                            all_terms: false,
                            phrase: false,
                            weight: 1.0,
                            prefetch_limit: None,
                        }),
                        offset: 0,
                        limit: 10,
                        rrf_k: 60.0,
                        fusion: rushdb_protocol::Fusion::Rrf,
                    },
                },
            },
        })
        .unwrap()
        .response;
    let Response::Records { records, total, .. } = response else {
        panic!("expected records, got {response:?}");
    };
    assert_eq!(total, 2, "{records:?}");
    let tags: Vec<&str> = records
        .iter()
        .map(|r| r["properties"]["tag"].as_str().unwrap())
        .collect();
    assert!(
        tags.iter().all(|&t| t == "keep"),
        "predicate must exclude `drop` from both legs, got {tags:?}"
    );
    // The strong-match record ranks first: it leads both the vector and the
    // lexical ranking, so RRF gives it the higher fused score.
    assert!(records[0]["score"].as_f64().unwrap() > records[1]["score"].as_f64().unwrap());
    // Both legs ranked it: each leg's own score rides along the fused one.
    assert!(records[0]["vectorScore"].as_f64().unwrap() > 0.99);
    assert!(records[0]["textScore"].as_f64().unwrap() > 0.0);
    let provenance = records[0]["provenance"].as_array().unwrap();
    assert!(provenance
        .iter()
        .any(|p| p.as_str().unwrap().starts_with("vector:")));
    assert!(provenance
        .iter()
        .any(|p| p.as_str().unwrap().starts_with("lexical:")));
}

/// A hybrid query with only a text leg ranks by BM25: each row's `score`
/// is its `textScore`, not a reciprocal-rank value, and `textSearch` counts
/// every match inside the filtered candidates, beyond the returned page.
/// `allTerms` and `phrase` reach the lexical leg.
#[test]
fn hybrid_text_only_scores_by_bm25_and_reports_matches() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    text_index(&node, "docs", "Doc", "immediate");
    for (tag, body) in [
        ("keep", "the quick brown fox"),
        ("keep", "quick thinking"),
        ("keep", "brown bread"),
        ("drop", "the quick brown fox"),
    ] {
        node.scope()
            .create_record(
                "Doc",
                &serde_json::json!({ "tag": tag, "body": body }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let search = |query: &str, all_terms: bool, phrase: bool, limit: u64| {
        let response = run(
            &node,
            Operation::Query {
                query: Query::Hybrid {
                    query: rushdb_protocol::HybridQuery {
                        labels: vec!["Doc".into()],
                        predicate: serde_json::json!({ "tag": "keep" }),
                        vector: None,
                        text: Some(rushdb_protocol::HybridTextLeg {
                            index: "docs".into(),
                            query: query.into(),
                            all_terms,
                            phrase,
                            weight: 1.0,
                            prefetch_limit: None,
                        }),
                        offset: 0,
                        limit,
                        rrf_k: 60.0,
                        fusion: rushdb_protocol::Fusion::Rrf,
                    },
                },
            },
        );
        let Response::Records {
            records,
            text_search,
            vector_search,
            ..
        } = response
        else {
            panic!("expected records, got {response:?}");
        };
        assert!(vector_search.is_none());
        (records, text_search.expect("textSearch report"))
    };

    let (records, report) = search("quick brown", false, false, 1);
    assert_eq!(report.matches, 3, "every `keep` record matches one term");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["score"], records[0]["textScore"]);
    assert!(records[0]["score"].as_f64().unwrap() > 0.1);
    assert!(records[0].get("vectorScore").is_none());

    let (_, all_terms) = search("quick brown", true, false, 10);
    assert_eq!(all_terms.matches, 1);
    let (_, phrase) = search("brown quick", false, true, 10);
    assert_eq!(
        phrase.matches, 0,
        "the words are not adjacent in that order"
    );
}

/// Three `Doc`s with a vector and a text index. Vector order for `[1, 0]`:
/// a, b, c. Text order for "fox": c, b (a has no fox).
fn fusion_fixture() -> Node {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
            label: "Doc".into(),
            property: "embedding".into(),
            similarity: "cosine".into(),
            dimensions: 2,
            name: Some("doc-embedding".into()),
            model: None,
            owner: None,
        })
        .unwrap();
    text_index(&node, "docs", "Doc", "immediate");
    for (tag, body, vector) in [
        ("a", "a lazy dog", [1.0_f32, 0.0]),
        ("b", "a fox", [0.8, 0.6]),
        ("c", "fox fox fox", [0.0, 1.0]),
    ] {
        node.scope()
            .create_record(
                "Doc",
                &serde_json::json!({ "tag": tag, "body": body }),
                &[serde_json::json!({
                    "propertyName": "embedding",
                    "vector": vector,
                    "index": "doc-embedding",
                })],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    node
}

/// A hybrid query of [`fusion_fixture`] with these fusion parameters.
fn fusion_query(
    node: &Node,
    fusion: rushdb_protocol::Fusion,
    (vector_weight, text_weight): (f64, f64),
    text_prefetch: Option<u64>,
) -> Result<Response, ProtocolError> {
    let query = rushdb_protocol::HybridQuery {
        labels: vec!["Doc".into()],
        predicate: serde_json::json!({}),
        vector: Some(rushdb_protocol::HybridVectorLeg {
            property: "embedding".into(),
            vector: vec![1.0, 0.0],
            similarity: "cosine".into(),
            index: Some("doc-embedding".into()),
            ef: None,
            mode: None,
            score_threshold: None,
            weight: vector_weight,
            prefetch_limit: None,
        }),
        text: Some(rushdb_protocol::HybridTextLeg {
            index: "docs".into(),
            query: "fox".into(),
            all_terms: false,
            phrase: false,
            weight: text_weight,
            prefetch_limit: text_prefetch,
        }),
        offset: 0,
        limit: 10,
        fusion,
        rrf_k: 60.0,
    };
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Hybrid { query },
            },
        })
        .map(|envelope| envelope.response)
}

/// Fusion parameters reach the hybrid search: a text leg's `prefetchLimit`
/// bounds what it ranks, a zero text weight leaves the vector order, DBSF
/// scores stay within the sum of the weights, and bad values are refused.
#[test]
fn hybrid_fusion_honours_prefetch_limits_weights_and_method() {
    use rushdb_protocol::Fusion;
    let node = fusion_fixture();
    let records = |response: Response| match response {
        Response::Records {
            records, fusion, ..
        } => (records, fusion),
        other => panic!("expected records, got {other:?}"),
    };
    let tags = |records: &[serde_json::Value]| {
        records
            .iter()
            .map(|r| r["properties"]["tag"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };

    // The text leg ranks only its best hit: `b` comes from the vector leg alone.
    let (limited, fusion) = records(fusion_query(&node, Fusion::Rrf, (1.0, 1.0), Some(1)).unwrap());
    assert_eq!(fusion, Some(Fusion::Rrf));
    let b = limited
        .iter()
        .find(|r| r["properties"]["tag"] == "b")
        .unwrap();
    assert_eq!(
        b["provenance"],
        serde_json::json!(["vector:1"]),
        "{limited:?}"
    );

    // A zero text weight leaves the vector order, for both methods.
    for method in [Fusion::Rrf, Fusion::Dbsf] {
        let (vector_only, _) = records(fusion_query(&node, method, (1.0, 0.0), None).unwrap());
        assert_eq!(tags(&vector_only), vec!["a", "b", "c"], "{method:?}");
    }

    // DBSF: normalized scores, so the sum of the weights bounds each one.
    let (dbsf, fusion) = records(fusion_query(&node, Fusion::Dbsf, (1.0, 0.5), None).unwrap());
    assert_eq!(fusion, Some(Fusion::Dbsf));
    for record in &dbsf {
        let score = record["score"].as_f64().unwrap();
        assert!((0.0..=1.5).contains(&score), "{record}");
    }

    for (weights, prefetch, expected) in [
        ((-1.0, 1.0), None, "`weight` must be a finite number"),
        ((0.0, 0.0), None, "at least one leg `weight`"),
        (
            (1.0, 1.0),
            Some(0),
            "`prefetchLimit` must be between 1 and 10000",
        ),
        (
            (1.0, 1.0),
            Some(10_001),
            "`prefetchLimit` must be between 1 and 10000",
        ),
    ] {
        let error = fusion_query(&node, Fusion::Rrf, weights, prefetch).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidRequest, "{error:?}");
        assert!(error.message.contains(expected), "{error:?}");
    }
}

/// `HybridQuery.offset` pages into the fused ranking rather than just
/// truncating it: page 2 (`offset == limit`, same `limit`) must match the
/// tail of one unpaginated fetch of `offset + limit` results exactly,
/// element for element. Ten records get strictly decreasing cosine
/// similarity to the query vector (monotonically increasing angle), so the
/// fused ranking has no score ties to make the comparison ambiguous.
#[test]
fn hybrid_query_offset_pages_match_tail_of_unpaginated_fetch() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
            label: "Doc".into(),
            property: "embedding".into(),
            similarity: "cosine".into(),
            dimensions: 2,
            name: Some("doc-embedding".into()),
            model: None,
            owner: None,
        })
        .unwrap();

    for i in 0..10u32 {
        let theta = f64::from(i) * 0.05;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "theta is a small, bounded angle; the f32 embedding only needs \
                      enough precision to keep cosine similarity strictly ordered"
        )]
        let vector = [theta.cos() as f32, theta.sin() as f32];
        node.scope()
            .create_record(
                "Doc",
                &serde_json::json!({ "idx": i }),
                &[serde_json::json!({
                    "propertyName": "embedding",
                    "vector": vector,
                    "index": "doc-embedding",
                })],
                &serde_json::json!({}),
            )
            .unwrap();
    }

    let fetch = |offset: u64, limit: u64| -> Vec<i64> {
        let response = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Query {
                    query: Query::Hybrid {
                        query: rushdb_protocol::HybridQuery {
                            labels: vec!["Doc".into()],
                            predicate: serde_json::json!({}),
                            vector: Some(rushdb_protocol::HybridVectorLeg {
                                property: "embedding".into(),
                                vector: vec![1.0, 0.0],
                                similarity: "cosine".into(),
                                index: Some("doc-embedding".into()),
                                ef: None,
                                mode: None,
                                score_threshold: None,
                                weight: 1.0,
                                prefetch_limit: None,
                            }),
                            text: None,
                            offset,
                            limit,
                            rrf_k: 60.0,
                            fusion: rushdb_protocol::Fusion::Rrf,
                        },
                    },
                },
            })
            .unwrap()
            .response;
        let Response::Records { records, .. } = response else {
            panic!("expected records, got {response:?}");
        };
        records
            .iter()
            .map(|r| r["properties"]["idx"].as_i64().unwrap())
            .collect()
    };

    let unpaginated = fetch(0, 10);
    assert_eq!(
        unpaginated,
        (0..10).collect::<Vec<i64>>(),
        "exact leg must rank by strictly decreasing cosine similarity"
    );

    let page1 = fetch(0, 5);
    let page2 = fetch(5, 5);
    assert_eq!(page1, unpaginated[..5]);
    assert_eq!(
        page2,
        unpaginated[5..10],
        "offset=limit page must match the tail of the unpaginated offset+limit fetch"
    );
}

/// Index lifecycle across deletes and re-uploads: a `ready` index whose
/// records are deleted reports 0 indexed after stats self-healing, and
/// re-uploading data makes `indexedRecords` lag `totalRecords` (so Core's
/// reconcile can return it to `pending`).
#[test]
fn vector_index_stats_self_heal_across_delete_and_reupload() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
            label: "Doc".into(),
            property: "text".into(),
            similarity: "cosine".into(),
            dimensions: 4,
            name: Some("doc-text-4".into()),
            model: None,
            owner: None,
        })
        .unwrap();
    let alias = "doc-text-4".to_string();
    let id_of = |rec: &serde_json::Value| rec.get("id").unwrap().as_str().unwrap().to_string();
    let r1 = node
        .scope()
        .create_record(
            "Doc",
            &serde_json::json!({ "text": "alpha" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let r2 = node
        .scope()
        .create_record(
            "Doc",
            &serde_json::json!({ "text": "beta" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .bind_field_vector(&alias, &id_of(&r1), "text", &[1.0, 0.0, 0.0, 0.0])
        .unwrap();
    node.scope()
        .bind_field_vector(&alias, &id_of(&r2), "text", &[0.0, 1.0, 0.0, 0.0])
        .unwrap();

    let stats_before = node.scope().vector_index_stats(&alias).unwrap();
    assert_eq!(
        stats_before
            .get("indexedRecords")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        2,
        "both records are indexed"
    );

    // Delete all data.
    node.scope().delete_record(&id_of(&r1)).unwrap();
    node.scope().delete_record(&id_of(&r2)).unwrap();

    // Managed-index reconciliation: revalidation scrubs deleted records'
    // bindings to Retired, then stats report only Ready bindings.
    node.scope().revalidate_vector_index(&alias).unwrap();
    let stats_after_delete = node.scope().vector_index_stats(&alias).unwrap();
    assert_eq!(
        stats_after_delete
            .get("indexedRecords")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(9),
        0,
        "deleted records are no longer indexed"
    );
    assert_eq!(
        stats_after_delete
            .get("totalRecords")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(9),
        0,
        "deleted records no longer count as total"
    );

    // Re-upload: total grows, indexed lags -> Core reconcile returns pending.
    node.scope()
        .create_record(
            "Doc",
            &serde_json::json!({ "text": "gamma" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let stats_after_reupload = node.scope().vector_index_stats(&alias).unwrap();
    assert!(
        stats_after_reupload
            .get("totalRecords")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
            > stats_after_reupload
                .get("indexedRecords")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        "re-uploaded record must appear as pending work (total > indexed)"
    );
}

#[test]
fn schema_description_is_typed_and_record_scoped() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Describe {
                describe: Describe::Schema {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();
    let Response::Schema { schema } = response.response else {
        panic!("expected schema response")
    };
    assert_eq!(schema.labels.len(), 2);
    assert!(schema.labels.iter().all(|label| label.records_count == 1));
    // Properties are attributed per label: one `name` entry per label,
    // each scoped to its own record count.
    assert_eq!(schema.properties.len(), 2);
    assert!(schema
        .properties
        .iter()
        .all(|property| property.name == "name"));
    let person_name = schema
        .properties
        .iter()
        .find(|property| property.label.as_deref() == Some("Person"))
        .expect("person name property");
    let company_name = schema
        .properties
        .iter()
        .find(|property| property.label.as_deref() == Some("Company"))
        .expect("company name property");
    assert_eq!(person_name.records_count, 1);
    assert_eq!(company_name.records_count, 1);
    assert!(!person_name.is_array);
    assert!(schema.relationships.is_empty());
}

#[test]
fn schema_describes_array_typed_properties() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Ship",
            &serde_json::json!({ "name": "Falcon", "crew": ["han", "chewie"] }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(
            "Ship",
            &serde_json::json!({ "name": "X-wing", "crew": "rey" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Describe {
                describe: Describe::Schema {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();
    let Response::Schema { schema } = response.response else {
        panic!("expected schema response")
    };
    let crew = schema
        .properties
        .iter()
        .find(|property| property.name == "crew")
        .expect("crew property");
    assert!(crew.is_array, "crew stores an array on one record");
    assert_eq!(crew.value_type, "string");
    let name = schema
        .properties
        .iter()
        .find(|property| property.name == "name")
        .expect("name property");
    assert!(!name.is_array, "name is scalar on all records");
}

/// Integers and floats are one `number` to clients. The engine stores them in
/// two definitions (integers first, floats once one arrives), but describes
/// one entry per label with the exact combined count, the merged range, and
/// one id across labels, so a client keyed by (name, type) sees it once.
#[test]
fn integer_and_float_definitions_are_described_as_one_number() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for (label, hours) in [
        ("Battle", serde_json::json!(3)),
        ("Battle", serde_json::json!(5)),
        ("Battle", serde_json::json!(1.5)),
        ("Siege", serde_json::json!(48)),
    ] {
        node.scope()
            .create_record(
                label,
                &serde_json::json!({ "hours": hours, "name": label }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    for detail in [None, Some(rushdb_protocol::PropertyDetail::Catalog)] {
        let Response::Properties { properties } = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Describe {
                    describe: Describe::Properties {
                        query: QueryDocument::default(),
                        detail,
                        skip: None,
                        limit: None,
                    },
                },
            })
            .unwrap()
            .response
        else {
            panic!("expected properties response")
        };
        let hours: Vec<_> = properties
            .iter()
            .filter(|property| property.name == "hours")
            .collect();
        let per_label: Vec<_> = hours
            .iter()
            .map(|p| {
                (
                    p.label.clone().unwrap(),
                    p.value_type.clone(),
                    p.records_count,
                )
            })
            .collect();
        assert_eq!(
            per_label,
            vec![
                ("Battle".to_string(), "number".to_string(), 3),
                ("Siege".to_string(), "number".to_string(), 1),
            ],
            "{detail:?}"
        );
        assert_eq!(
            hours[0].id, hours[1].id,
            "one id across labels ({detail:?})"
        );
        assert!(
            hours.iter().all(|p| p.sibling_types.is_empty()),
            "{detail:?}"
        );
        if detail.is_none() {
            assert_eq!(hours[0].min, Some(serde_json::json!(1.5)));
            assert_eq!(hours[0].max, Some(serde_json::json!(5)));
        }
    }
}

#[test]
fn schema_describes_bounded_samples_and_exact_min_max() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for (rank, launched) in [
        (3, "1996-01-15T00:00:00.000Z"),
        (1, "1965-08-01T00:00:00.000Z"),
        (2, "1979-06-30T00:00:00.000Z"),
    ] {
        node.scope()
            .create_record(
                "Ship",
                &serde_json::json!({
                    "name": format!("ship-{rank}"),
                    "rank": rank,
                    "launched": launched,
                }),
                &[],
                &serde_json::json!({ "suggestTypes": true }),
            )
            .unwrap();
    }
    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Describe {
                describe: Describe::Schema {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();
    let Response::Schema { schema } = response.response else {
        panic!("expected schema response")
    };
    let rank = schema
        .properties
        .iter()
        .find(|property| property.name == "rank")
        .expect("rank property");
    assert_eq!(rank.records_count, 3);
    assert_eq!(rank.min, Some(serde_json::json!(1)));
    assert_eq!(rank.max, Some(serde_json::json!(3)));
    assert!(rank.sample_values.contains(&serde_json::json!(2)));
    let name = schema
        .properties
        .iter()
        .find(|property| property.name == "name")
        .expect("name property");
    assert_eq!(name.min, Some(serde_json::json!("ship-1")));
    assert_eq!(name.max, Some(serde_json::json!("ship-3")));
    // Datetimes order by instant and render as the records response shows
    // them, not as stored epoch milliseconds
    let launched = schema
        .properties
        .iter()
        .find(|property| property.name == "launched")
        .expect("launched property");
    assert_eq!(launched.value_type, "datetime");
    // (a midnight-UTC instant renders as its date).
    assert_eq!(launched.min, Some(serde_json::json!("1965-08-01")));
    assert_eq!(launched.max, Some(serde_json::json!("1996-01-15")));
    assert!(launched
        .sample_values
        .contains(&serde_json::json!("1979-06-30")));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
)]
fn describe_properties_catalog_matches_full_counts_without_record_scan() {
    // E2+E7: catalog detail answers names, exact support counts and
    // array shape from catalog indexes alone. Scalar, multi-element
    // array, single-element array (the keys-only ambiguity) and an
    // empty array must all agree with the full scan.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for (i, doc) in [
        serde_json::json!({"n": 1, "tags": ["a", "b"], "single": [1], "empty": []}),
        serde_json::json!({"n": 2, "tags": ["c"], "single": [2], "empty": []}),
        serde_json::json!({"n": 3, "tags": "solo"}),
    ]
    .into_iter()
    .enumerate()
    {
        node.scope()
            .create_record(
                if i < 2 { "A" } else { "B" },
                &doc,
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let describe = |detail: Option<rushdb_protocol::PropertyDetail>| {
        let Response::Properties { properties } = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Describe {
                    describe: Describe::Properties {
                        query: QueryDocument::default(),
                        detail,
                        skip: None,
                        limit: None,
                    },
                },
            })
            .unwrap()
            .response
        else {
            panic!("expected properties response")
        };
        properties
    };
    let full = describe(None);
    let catalog = describe(Some(rushdb_protocol::PropertyDetail::Catalog));
    // Same entry identities.
    let mut full_keys: Vec<(String, String)> = full
        .iter()
        .map(|p| (p.label.clone().unwrap_or_default(), p.name.clone()))
        .collect();
    full_keys.sort();
    let mut catalog_keys: Vec<(String, String)> = catalog
        .iter()
        .map(|p| (p.label.clone().unwrap_or_default(), p.name.clone()))
        .collect();
    catalog_keys.sort();
    assert_eq!(full_keys, catalog_keys, "catalog must cover every entry");
    // Same counts and array shape per entry (E7 exactness, incl. the
    // single-element-array disambiguation).
    for entry in &catalog {
        let key = (entry.label.clone().unwrap_or_default(), entry.name.clone());
        let peer = full
            .iter()
            .find(|p| {
                (p.label.clone().unwrap_or_default(), p.name.clone()) == key
                    && p.value_type == entry.value_type
            })
            .unwrap_or_else(|| panic!("catalog entry without full peer: {key:?}"));
        assert_eq!(
            entry.records_count, peer.records_count,
            "support mismatch for {key:?}"
        );
        assert_eq!(
            entry.is_array, peer.is_array,
            "array shape mismatch for {key:?}"
        );
        assert!(
            entry.sample_values.is_empty() && entry.min.is_none() && entry.max.is_none(),
            "catalog entries carry no scan statistics"
        );
    }
    // Spot-check the interesting shapes directly.
    let tags_a = catalog
        .iter()
        .find(|p| p.name == "tags" && p.label.as_deref() == Some("A"))
        .expect("tags on A");
    assert_eq!(tags_a.records_count, 2);
    assert!(tags_a.is_array);
    let single_a = catalog
        .iter()
        .find(|p| p.name == "single" && p.label.as_deref() == Some("A"))
        .expect("single on A");
    assert_eq!(single_a.records_count, 2);
    assert!(
        single_a.is_array,
        "single-element arrays are arrays, not scalars"
    );
    let tags_b = catalog
        .iter()
        .find(|p| p.name == "tags" && p.label.as_deref() == Some("B"))
        .expect("tags on B");
    assert_eq!(tags_b.records_count, 1);
    assert!(!tags_b.is_array, "scalar stays scalar");
    // A filtered catalog read is refused (filtering needs the scan).
    let filtered = ProtocolExecutor::new(&node, None).execute(RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Describe {
            describe: Describe::Properties {
                query: QueryDocument {
                    predicate: serde_json::json!({"n": 1}),
                    ..Default::default()
                },
                detail: Some(rushdb_protocol::PropertyDetail::Catalog),
                skip: None,
                limit: None,
            },
        },
    });
    assert!(
        filtered.is_err(),
        "catalog detail with `where` must be refused"
    );
}

#[test]
fn describe_properties_entry_pagination_slices_sorted_entries() {
    // E2: skip/limit slice the sorted entry list on both detail levels.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "P",
            &serde_json::json!({"zeta": 1, "alpha": 2, "mid": 3}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let page = |skip, limit, detail| {
        let Response::Properties { properties } = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Describe {
                    describe: Describe::Properties {
                        query: QueryDocument::default(),
                        detail,
                        skip,
                        limit,
                    },
                },
            })
            .unwrap()
            .response
        else {
            panic!("expected properties response")
        };
        properties
            .iter()
            .map(|p| p.name.clone())
            .collect::<Vec<_>>()
    };
    for detail in [None, Some(rushdb_protocol::PropertyDetail::Catalog)] {
        assert_eq!(page(None, None, detail), vec!["alpha", "mid", "zeta"]);
        assert_eq!(page(Some(1), Some(1), detail), vec!["mid"]);
        assert!(page(Some(9), None, detail).is_empty());
    }
}

#[test]
fn describe_properties_flags_type_splits() {
    // E8: a name arriving under a second display type keeps both
    // definitions and each entry flags the other as a sibling, so a
    // client cached on one type detects partitioned support. (Integers and
    // floats are one `number`; see integer_and_float_definitions_are_described_as_one_number.)
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for x in [serde_json::json!(1), serde_json::json!("A-1")] {
        node.scope()
            .create_record(
                "M",
                &serde_json::json!({ "x": x }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    for detail in [None, Some(rushdb_protocol::PropertyDetail::Catalog)] {
        let Response::Properties { properties } = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Describe {
                    describe: Describe::Properties {
                        query: QueryDocument::default(),
                        detail,
                        skip: None,
                        limit: None,
                    },
                },
            })
            .unwrap()
            .response
        else {
            panic!("expected properties response")
        };
        let mut entries: Vec<(String, Vec<String>)> = properties
            .iter()
            .filter(|p| p.name == "x")
            .map(|p| (p.value_type.clone(), p.sibling_types.clone()))
            .collect();
        entries.sort();
        assert_eq!(
            entries,
            vec![
                ("number".to_string(), vec!["utf8".to_string()]),
                ("string".to_string(), vec!["int64".to_string()]),
            ],
            "{detail:?}"
        );
    }
}

#[test]
fn property_values_modes_serve_count_range_and_truncated_values() {
    // E3: Count/Range answer without shipping distinct sets; Values
    // honors `limit` with an exact total and a truncation flag. The
    // default (no mode/limit) is the full list, ranked by record count.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for rank in [3, 1, 2, 2] {
        node.scope()
            .create_record(
                "Ship",
                &serde_json::json!({ "rank": rank }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let ask = |mode, limit| {
        let Response::PropertyValues {
            property,
            values,
            distinct_count,
            min,
            max,
            truncated,
            ..
        } = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Query {
                    query: Query::PropertyValues {
                        property: "rank".into(),
                        query: QueryDocument::default(),
                        mode,
                        limit,
                        search: None,
                    },
                },
            })
            .unwrap()
            .response
        else {
            panic!("expected propertyValues response")
        };
        assert_eq!(property, "rank");
        (values, distinct_count, min, max, truncated)
    };
    // Default: the full distinct list, by descending record count, ties by
    // value (written 3, 1, 2, 2).
    let (values, count, min, max, truncated) = ask(None, None);
    assert_eq!(
        values,
        vec![
            serde_json::json!(2),
            serde_json::json!(1),
            serde_json::json!(3)
        ]
    );
    assert_eq!(count, Some(3));
    assert_eq!((min, max), (None, None));
    assert!(!truncated);
    // Truncated values: the head of that order, exact total preserved, flag set.
    let (values, count, _, _, truncated) = ask(None, Some(2));
    assert_eq!(values, vec![serde_json::json!(2), serde_json::json!(1)]);
    assert_eq!(count, Some(3));
    assert!(truncated);
    // Count: no values shipped.
    let (values, count, _, _, truncated) =
        ask(Some(rushdb_protocol::PropertyValuesMode::Count), None);
    assert!(values.is_empty());
    assert_eq!(count, Some(3));
    assert!(!truncated);
    // Range: extrema only.
    let (values, count, min, max, _) = ask(Some(rushdb_protocol::PropertyValuesMode::Range), None);
    assert!(values.is_empty());
    assert_eq!(count, Some(3));
    assert_eq!(
        (min, max),
        (Some(serde_json::json!(1)), Some(serde_json::json!(3)))
    );
}

#[test]
fn top_k_is_exact() {
    use rushdb_protocol::PropertyValuesMode;
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for rank in [3, 1, 2, 2] {
        node.scope()
            .create_record(
                "Ship",
                &serde_json::json!({ "rank": rank }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let ask = |mode: Option<PropertyValuesMode>, limit: Option<u64>| {
        let Response::PropertyValues {
            values,
            distinct_count,
            counts,
            ..
        } = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Query {
                    query: Query::PropertyValues {
                        property: "rank".into(),
                        query: QueryDocument::default(),
                        mode,
                        limit,
                        search: None,
                    },
                },
            })
            .unwrap()
            .response
        else {
            panic!("expected propertyValues response")
        };
        (values, distinct_count, counts)
    };
    // TopK is exact: rank 2 appears twice, the singletons are tie-broken
    // by canonical JSON.
    let (values, count, counts) = ask(Some(PropertyValuesMode::TopK), Some(2));
    assert_eq!(count, Some(3));
    assert_eq!(values, vec![serde_json::json!(2), serde_json::json!(1)]);
    assert_eq!(counts, vec![2, 1]);
    // Count is exact and distinct (no approximate mode exists).
    let (values, count, _) = ask(Some(PropertyValuesMode::Count), None);
    assert!(values.is_empty());
    assert_eq!(count, Some(3));
}

#[test]
fn property_values_range_keeps_large_integer_extrema_exact() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for value in [9_007_199_254_740_993_i64, 9_007_199_254_740_992_i64] {
        node.scope()
            .create_record(
                "LargeNumber",
                &serde_json::json!({ "value": value }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let Response::PropertyValues { min, max, .. } = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::PropertyValues {
                    property: "value".into(),
                    query: QueryDocument::default(),
                    mode: Some(rushdb_protocol::PropertyValuesMode::Range),
                    limit: None,
                    search: None,
                },
            },
        })
        .unwrap()
        .response
    else {
        panic!("expected propertyValues response")
    };
    assert_eq!(min, Some(serde_json::json!(9_007_199_254_740_992_i64)));
    assert_eq!(max, Some(serde_json::json!(9_007_199_254_740_993_i64)));
}

#[test]
fn schema_catalog_detail_projects_and_paginates_property_entries() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Widget",
            &serde_json::json!({"alpha": 1, "beta": "x"}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(
            "Widget",
            &serde_json::json!({"gamma": true}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let run = |detail, skip, limit| {
        let Response::Schema { schema } = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Describe {
                    describe: Describe::Schema {
                        query: QueryDocument::default(),
                        detail,
                        skip,
                        limit,
                    },
                },
            })
            .unwrap()
            .response
        else {
            panic!("expected schema response")
        };
        schema
    };
    let full = run(None, None, None);
    assert!(full.properties.len() >= 3, "{:?}", full.properties);
    // Catalog detail: no samples/extrema, support counts still exact.
    let catalog = run(Some(rushdb_protocol::PropertyDetail::Catalog), None, None);
    assert!(catalog
        .properties
        .iter()
        .all(|p| p.sample_values.is_empty() && p.min.is_none() && p.max.is_none()));
    assert!(catalog.properties.iter().all(|p| p.records_count >= 1));
    // Pagination slices the property entry list.
    let page = run(
        Some(rushdb_protocol::PropertyDetail::Catalog),
        Some(1),
        Some(1),
    );
    assert_eq!(page.properties.len(), 1);
    assert_eq!(page.properties[0].name, catalog.properties[1].name);
}

#[test]
fn delete_extremum_moves_describe_min_max() {
    // E9: summaries are recomputed per describe scan (no maintained
    // extrema), so deleting the min/max record must move the reported
    // extrema — worst case is one bounded scan, never a stale sketch.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for rank in [1, 2, 3] {
        node.scope()
            .create_record(
                "Ship",
                &serde_json::json!({ "rank": rank }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let min_max = || {
        let Response::Schema { schema } = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Describe {
                    describe: Describe::Schema {
                        query: QueryDocument::default(),
                        detail: None,
                        skip: None,
                        limit: None,
                    },
                },
            })
            .unwrap()
            .response
        else {
            panic!("expected schema response")
        };
        let rank = schema
            .properties
            .iter()
            .find(|p| p.name == "rank")
            .expect("rank property");
        (rank.min.clone(), rank.max.clone(), rank.records_count)
    };
    assert_eq!(
        min_max(),
        (Some(serde_json::json!(1)), Some(serde_json::json!(3)), 3)
    );
    let deleted = node
        .scope()
        .delete_records(&serde_json::json!({"labels": ["Ship"], "where": {"rank": 1}}))
        .unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(
        min_max(),
        (Some(serde_json::json!(2)), Some(serde_json::json!(3)), 2),
        "deleting the extremum must move min without stale summaries"
    );
}

#[test]
fn schema_description_includes_scoped_relationship_topology() {
    let mut cfg = config(true);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let scope = RequestScope::new(&node, Some("tenant-a".into()));
    scope
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada", "join": 1 }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    scope
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB", "join": 1 }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    scope
        .link_records(&rushdb_protocol::RelationshipMutation {
            relation_type: "FOUNDED".into(),
            source: rushdb_protocol::RelationshipSelector {
                label: "Person".into(),
                property: Some("join".into()),
                predicate: serde_json::json!({}),
            },
            target: rushdb_protocol::RelationshipSelector {
                label: "Company".into(),
                property: Some("join".into()),
                predicate: serde_json::json!({}),
            },
            direction: rushdb_protocol::Direction::Out,
            allow_cartesian: false,
            properties: serde_json::json!({ "since": 2020 }),
        })
        .unwrap();

    let response = ProtocolExecutor::new(&node, Some("tenant-a".into()))
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: Some("tenant-a".into()),
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Describe {
                describe: Describe::Schema {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();

    let Response::Schema { schema } = response.response else {
        panic!("expected schema response")
    };
    assert!(schema
        .properties
        .iter()
        .all(|property| property.name != "tenantKey"));
    assert_eq!(schema.relationships.len(), 1);
    let relationship = &schema.relationships[0];
    assert_eq!(relationship.source_label, "Person");
    assert_eq!(relationship.target_label, "Company");
    assert_eq!(relationship.relationship_type, "FOUNDED");
    assert_eq!(relationship.count, 1);
    assert_eq!(relationship.properties.len(), 1);
    assert_eq!(relationship.properties[0].name, "since");
    assert_eq!(relationship.properties[0].value_type, "number");
}

#[test]
fn blessed_discovery_shapes_use_catalog() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for i in 0..3 {
        node.scope()
            .create_record(
                "Person",
                &serde_json::json!({ "name": format!("p{i}"), "join": i }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    node.scope()
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB", "join": 1 }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .link_records(&rushdb_protocol::RelationshipMutation {
            relation_type: "FOUNDED".into(),
            source: rushdb_protocol::RelationshipSelector {
                label: "Person".into(),
                property: Some("join".into()),
                predicate: serde_json::json!({}),
            },
            target: rushdb_protocol::RelationshipSelector {
                label: "Company".into(),
                property: Some("join".into()),
                predicate: serde_json::json!({}),
            },
            direction: rushdb_protocol::Direction::Out,
            allow_cartesian: false,
            properties: serde_json::json!({}),
        })
        .unwrap();

    let schema = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Describe {
                describe: Describe::Schema {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();
    let Response::Schema { schema } = schema.response else {
        panic!("expected schema")
    };
    assert_eq!(schema.relationships.len(), 1);
    assert_eq!(schema.relationships[0].count, 1);
    assert_eq!(schema.relationships[0].relationship_type, "FOUNDED");

    let counted = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Records {
                    query: QueryDocument {
                        labels: vec!["Person".into()],
                        select: Some(serde_json::json!({ "n": { "$count": "*" } })),
                        ..QueryDocument::default()
                    },
                },
            },
        })
        .unwrap();
    let Response::Rows { rows, total } = counted.response else {
        panic!("expected aggregate rows, got {:?}", counted.response)
    };
    assert_eq!(total, 3);
    assert_eq!(rows[0]["n"], 3);
}

#[test]
fn link_and_unlink_records_by_id_are_idempotent_and_typed() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let lmpg = node.scope().lmpg();
    let snap = lmpg.store.read_view().unwrap();
    let persons = test_access::label_members(&snap, lmpg.tenant, "Person");
    let companies = test_access::label_members(&snap, lmpg.tenant, "Company");
    let id_of = |ordinal| {
        let record = node.scope().record_value(ordinal).unwrap();
        record["id"].as_str().unwrap().to_string()
    };
    let pid = id_of(persons[0]);
    let cid = id_of(companies[0]);

    let affected = |mutation: rushdb_protocol::Mutation| {
        let response = ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext {
                    scope: None,
                    deadline_ms: None,
                    ..Default::default()
                },
                operation: Operation::Mutate { mutation },
            })
            .unwrap()
            .response;
        match response {
            Response::Mutation { affected }
            | Response::Linked {
                created: affected, ..
            } => affected,
            other => panic!("unexpected response {other:?}"),
        }
    };
    let link = |command| rushdb_protocol::Mutation::LinkRecordsById { command };
    let unlink = |command| rushdb_protocol::Mutation::UnlinkRecordsById { command };

    let cmd = |ids: Vec<String>| rushdb_protocol::RelationshipIdsMutation {
        relation_type: "FOUNDED".into(),
        direction: rushdb_protocol::Direction::Out,
        source_ids: vec![pid.clone()],
        target_ids: ids,
        properties: serde_json::json!({ "since": 2020 }),
    };

    // First link creates 1; a second identical link is idempotent (0 new).
    assert_eq!(
        affected(link(cmd(vec![cid.clone()]))),
        1,
        "first by-id link creates one edge"
    );
    assert_eq!(
        affected(link(cmd(vec![cid.clone()]))),
        0,
        "by-id link is idempotent per pair"
    );

    let unlink_founded = |direction| {
        affected(unlink(rushdb_protocol::UnlinkRecordsMutation {
            relation_types: vec!["FOUNDED".into()],
            direction: Some(direction),
            source_ids: vec![pid.clone()],
            target_ids: vec![cid.clone()],
        }))
    };
    let (out, inward) = (
        rushdb_protocol::Direction::Out,
        rushdb_protocol::Direction::In,
    );

    // Unlink deletes the typed edge; a repeat unlink is a no-op.
    assert_eq!(unlink_founded(out), 1, "by-id unlink removes the edge");
    assert_eq!(unlink_founded(out), 0, "by-id unlink is idempotent");

    // `direction: In` links target → source; unlink honours the direction.
    let inbound = rushdb_protocol::RelationshipIdsMutation {
        direction: inward,
        ..cmd(vec![cid.clone()])
    };
    assert_eq!(affected(link(inbound)), 1);
    assert_eq!(
        unlink_founded(out),
        0,
        "an Out unlink must not delete the In edge"
    );
    assert_eq!(
        unlink_founded(inward),
        1,
        "an In unlink deletes the In edge"
    );
}

#[test]
fn link_records_by_id_commits_one_batch_with_counts() {
    // E1: N edges in one call must cost exactly one commit (not N), with
    // created/skipped_existing distinguishing new edges from duplicates.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for name in ["a1", "a2"] {
        node.scope()
            .create_record(
                "Person",
                &serde_json::json!({ "name": name }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    for name in ["c1", "c2"] {
        node.scope()
            .create_record(
                "Company",
                &serde_json::json!({ "name": name }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let lmpg = node.scope().lmpg();
    let snap = lmpg.store.read_view().unwrap();
    let ids = |label: &str| {
        test_access::label_members(&snap, lmpg.tenant, label)
            .iter()
            .map(|ord| {
                node.scope()
                    .record_value(*ord)
                    .unwrap()
                    .get("id")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>()
    };
    let persons = ids("Person");
    let companies = ids("Company");
    assert_eq!((persons.len(), companies.len()), (2, 2));

    let execute = |mutation: rushdb_protocol::Mutation| {
        ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext {
                    scope: None,
                    deadline_ms: None,
                    ..Default::default()
                },
                operation: Operation::Mutate { mutation },
            })
            .unwrap()
            .response
    };
    let cmd = |targets: Vec<String>| rushdb_protocol::RelationshipIdsMutation {
        relation_type: "WORKS_AT".into(),
        direction: rushdb_protocol::Direction::Out,
        source_ids: persons.clone(),
        target_ids: targets,
        properties: serde_json::json!({}),
    };
    let affected = |response: rushdb_protocol::Response| match response {
        rushdb_protocol::Response::Mutation { affected }
        | rushdb_protocol::Response::Linked {
            created: affected, ..
        } => affected,
        other => panic!("unexpected response {other:?}"),
    };

    let before = node.store().committed_version();
    let created = affected(execute(rushdb_protocol::Mutation::LinkRecordsById {
        command: cmd(companies.clone()),
    }));
    assert_eq!(created, 4, "2x2 by-id links create four edges");
    assert_eq!(
        node.store().committed_version(),
        before + 1,
        "one call must cost one commit, not one per edge"
    );

    // Re-linking is idempotent and stages nothing (no commit consumed).
    let before = node.store().committed_version();
    let created = affected(execute(rushdb_protocol::Mutation::LinkRecordsById {
        command: cmd(companies),
    }));
    assert_eq!(created, 0, "duplicate batch creates nothing");
    assert_eq!(
        node.store().committed_version(),
        before,
        "all-duplicate batch must not consume a commit"
    );
}

#[test]
fn link_records_by_id_with_dangling_target_applies_nothing() {
    // E5: endpoint resolution fails fast before any write, so a batch
    // with one dangling id applies nothing; retrying with only the valid
    // target then creates exactly that edge.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let lmpg = node.scope().lmpg();
    let snap = lmpg.store.read_view().unwrap();
    let pid = node
        .scope()
        .record_value(test_access::label_members(&snap, lmpg.tenant, "Person")[0])
        .unwrap()
        .get("id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let cid = node
        .scope()
        .record_value(test_access::label_members(&snap, lmpg.tenant, "Company")[0])
        .unwrap()
        .get("id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();

    let try_execute = |targets: Vec<String>| {
        ProtocolExecutor::new(&node, None).execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: None,
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Mutate {
                mutation: rushdb_protocol::Mutation::LinkRecordsById {
                    command: rushdb_protocol::RelationshipIdsMutation {
                        relation_type: "FOUNDED".into(),
                        direction: rushdb_protocol::Direction::Out,
                        source_ids: vec![pid.clone()],
                        target_ids: targets,
                        properties: serde_json::json!({}),
                    },
                },
            },
        })
    };
    let dangling = "00000000000000000000000000000000".to_string();
    assert!(
        try_execute(vec![cid.clone(), dangling]).is_err(),
        "dangling target must fail the batch"
    );
    let retry = try_execute(vec![cid]).unwrap();
    let affected = match retry.response {
        rushdb_protocol::Response::Mutation { affected }
        | rushdb_protocol::Response::Linked {
            created: affected, ..
        } => affected,
        other => panic!("unexpected response {other:?}"),
    };
    assert_eq!(
        affected, 1,
        "retry creates the edge, proving the failed batch applied nothing"
    );
}

#[test]
fn link_records_pairs_commits_one_batch_with_verbatim_types() {
    // Pairwise delivery gate: N explicit pairs cost exactly one commit,
    // per-pair types survive verbatim (HAS_NEST_* default + REL_*
    // overrides in one call), and repeats are idempotent.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for name in ["a1", "a2", "b1"] {
        node.scope()
            .create_record(
                "T",
                &serde_json::json!({ "name": name }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let lmpg = node.scope().lmpg();
    let snap = lmpg.store.read_view().unwrap();
    let ids: Vec<String> = test_access::label_members(&snap, lmpg.tenant, "T")
        .iter()
        .map(|ord| {
            node.scope()
                .record_value(*ord)
                .unwrap()
                .get("id")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(ids.len(), 3);
    let execute = |mutation: rushdb_protocol::Mutation| {
        ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext {
                    scope: None,
                    deadline_ms: None,
                    ..Default::default()
                },
                operation: Operation::Mutate { mutation },
            })
            .unwrap()
    };
    let pairs = vec![
        rushdb_protocol::RelationshipPair {
            source_id: ids[0].clone(),
            target_id: ids[1].clone(),
            relation_type: None,
        },
        rushdb_protocol::RelationshipPair {
            source_id: ids[0].clone(),
            target_id: ids[2].clone(),
            relation_type: Some("REL_CUSTOM".into()),
        },
        rushdb_protocol::RelationshipPair {
            source_id: ids[1].clone(),
            target_id: ids[2].clone(),
            relation_type: Some("REL_OTHER".into()),
        },
    ];
    let cmd = || rushdb_protocol::RelationshipPairsMutation {
        relation_type: "HAS_NEST_X".into(),
        direction: rushdb_protocol::Direction::Out,
        pairs: pairs.clone(),
        properties: serde_json::json!({}),
    };
    let affected = |response: rushdb_protocol::Response| match response {
        rushdb_protocol::Response::Mutation { affected }
        | rushdb_protocol::Response::Linked {
            created: affected, ..
        } => affected,
        other => panic!("unexpected response {other:?}"),
    };
    let before = node.store().committed_version();
    let created =
        affected(execute(rushdb_protocol::Mutation::LinkRecordsPairs { command: cmd() }).response);
    assert_eq!(created, 3);
    assert_eq!(
        node.store().committed_version(),
        before + 1,
        "one pairs call must cost one commit"
    );
    // Ground-truth types survive verbatim: default + both overrides.
    let view = node.store().read_view().unwrap();
    for (ord, ty) in [(0, "HAS_NEST_X"), (0, "REL_CUSTOM"), (1, "REL_OTHER")] {
        let linked = rushdb_lmpg::reader::try_access::adj_out(&view, lmpg.tenant, ord, Some(ty))
            .unwrap()
            .len();
        assert_eq!(linked, 1, "verbatim {ty} edge missing for ord {ord}");
    }
    // Repeat is idempotent and stages nothing.
    let before = node.store().committed_version();
    let created =
        affected(execute(rushdb_protocol::Mutation::LinkRecordsPairs { command: cmd() }).response);
    assert_eq!(created, 0);
    assert_eq!(
        node.store().committed_version(),
        before,
        "all-duplicate pairs batch must not consume a commit"
    );
}

#[test]
fn link_records_pairs_with_dangling_id_applies_nothing() {
    // Delivery gate (a): bad-id batch fails loud with zero partials;
    // retrying without the bad id then creates exactly its edges.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let lmpg = node.scope().lmpg();
    let snap = lmpg.store.read_view().unwrap();
    let members = test_access::label_members(&snap, lmpg.tenant, "Person");
    let comembers = test_access::label_members(&snap, lmpg.tenant, "Company");
    let pid = node
        .scope()
        .record_value(members[0])
        .unwrap()
        .get("id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let cid = node
        .scope()
        .record_value(comembers[0])
        .unwrap()
        .get("id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let try_execute = |pairs: Vec<rushdb_protocol::RelationshipPair>| {
        ProtocolExecutor::new(&node, None).execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: None,
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Mutate {
                mutation: rushdb_protocol::Mutation::LinkRecordsPairs {
                    command: rushdb_protocol::RelationshipPairsMutation {
                        relation_type: "FOUNDED".into(),
                        direction: rushdb_protocol::Direction::Out,
                        pairs,
                        properties: serde_json::json!({}),
                    },
                },
            },
        })
    };
    let good = || rushdb_protocol::RelationshipPair {
        source_id: pid.clone(),
        target_id: cid.clone(),
        relation_type: None,
    };
    let dangling = rushdb_protocol::RelationshipPair {
        source_id: pid.clone(),
        target_id: "00000000000000000000000000000000".to_string(),
        relation_type: None,
    };
    assert!(
        try_execute(vec![good(), dangling]).is_err(),
        "batch with a dangling id must fail loud"
    );
    let retry = try_execute(vec![good()]).unwrap();
    let affected = match retry.response {
        rushdb_protocol::Response::Mutation { affected }
        | rushdb_protocol::Response::Linked {
            created: affected, ..
        } => affected,
        other => panic!("unexpected response {other:?}"),
    };
    assert_eq!(
        affected, 1,
        "retry creates the edge, proving the failed batch applied nothing"
    );
}

#[test]
fn link_records_pairs_do_not_disturb_other_shapes() {
    // Delivery gate (a), cross-shape non-interference: a pairs batch
    // neither skips over nor duplicates edges of other types, and a
    // by-id batch does not disturb pairs-created edges.
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for name in ["a", "b"] {
        node.scope()
            .create_record(
                "N",
                &serde_json::json!({ "name": name }),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    let lmpg = node.scope().lmpg();
    let snap = lmpg.store.read_view().unwrap();
    let ids: Vec<String> = test_access::label_members(&snap, lmpg.tenant, "N")
        .iter()
        .map(|ord| {
            node.scope()
                .record_value(*ord)
                .unwrap()
                .get("id")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    let execute = |mutation: rushdb_protocol::Mutation| {
        ProtocolExecutor::new(&node, None)
            .execute(RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: None,
                context: RequestContext {
                    scope: None,
                    deadline_ms: None,
                    ..Default::default()
                },
                operation: Operation::Mutate { mutation },
            })
            .unwrap()
            .response
    };
    let affected = |response: rushdb_protocol::Response| match response {
        rushdb_protocol::Response::Mutation { affected }
        | rushdb_protocol::Response::Linked {
            created: affected, ..
        } => affected,
        other => panic!("unexpected response {other:?}"),
    };
    // Seed one edge via the cross-product path.
    let seeded = affected(execute(rushdb_protocol::Mutation::LinkRecordsById {
        command: rushdb_protocol::RelationshipIdsMutation {
            relation_type: "SEED".into(),
            direction: rushdb_protocol::Direction::Out,
            source_ids: vec![ids[0].clone()],
            target_ids: vec![ids[1].clone()],
            properties: serde_json::json!({}),
        },
    }));
    assert_eq!(seeded, 1);
    // Pairs batch with a new type: creates its edge, leaves SEED alone.
    let created = affected(execute(rushdb_protocol::Mutation::LinkRecordsPairs {
        command: rushdb_protocol::RelationshipPairsMutation {
            relation_type: "PAIR".into(),
            direction: rushdb_protocol::Direction::Out,
            pairs: vec![rushdb_protocol::RelationshipPair {
                source_id: ids[0].clone(),
                target_id: ids[1].clone(),
                relation_type: None,
            }],
            properties: serde_json::json!({}),
        },
    }));
    assert_eq!(created, 1);
    // Same pair, same type via by-id: now a duplicate (skipped).
    let dup = affected(execute(rushdb_protocol::Mutation::LinkRecordsById {
        command: rushdb_protocol::RelationshipIdsMutation {
            relation_type: "PAIR".into(),
            direction: rushdb_protocol::Direction::Out,
            source_ids: vec![ids[0].clone()],
            target_ids: vec![ids[1].clone()],
            properties: serde_json::json!({}),
        },
    }));
    assert_eq!(dup, 0, "cross-shape duplicate must be detected");
    // Same pair under a third type via pairs: distinct edge, created.
    let created = affected(execute(rushdb_protocol::Mutation::LinkRecordsPairs {
        command: rushdb_protocol::RelationshipPairsMutation {
            relation_type: "OTHER".into(),
            direction: rushdb_protocol::Direction::Out,
            pairs: vec![rushdb_protocol::RelationshipPair {
                source_id: ids[0].clone(),
                target_id: ids[1].clone(),
                relation_type: None,
            }],
            properties: serde_json::json!({}),
        },
    }));
    assert_eq!(created, 1, "same pair under a new type is a distinct edge");
}

#[test]
fn transaction_statement_staging_is_gated_by_language() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let transaction_id = node.scope().tx_begin().unwrap();
    let err = ProtocolExecutor::new(&node, None)
        .execute(transaction_request(
            TransactionOperation::Stage {
                transaction_id,
                mutation: rushdb_protocol::TransactionMutation::Statement {
                    language: rushdb_protocol::StatementLanguage::Cypher,
                    statement: "CREATE (:Doc {n: 1})".into(),
                    parameters: None,
                },
            },
            None,
            None,
        ))
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::LanguageDisabled);
}

/// Language gating: searchquery is always on, a disabled language is refused
/// with `LanguageDisabled`, and an enabled language (mongodb) runs end-to-end.
#[test]
fn language_gating_enforces_enablement_and_searchquery_always_on() {
    use rushdb_protocol::StatementLanguage;

    // No statement languages: searchquery only.
    let mut cfg = config(false);
    cfg.languages = rushdb_protocol::LanguageSet::NONE;
    let node = Node::bootstrap(&mut cfg).unwrap();
    let executor = ProtocolExecutor::new(&node, None);
    let statement = |language: StatementLanguage, statement: &str| RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Query {
            query: Query::Statement {
                language,
                statement: statement.to_string(),
                parameters: None,
            },
        },
    };

    // searchquery (native QueryDocument) always works.
    let ok = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Records {
                    query: rushdb_protocol::QueryDocument::default(),
                },
            },
        })
        .unwrap();
    assert!(matches!(ok.response, Response::Records { .. }));

    // A disabled language (mongodb by default) is refused explicitly.
    let err = executor
        .execute(statement(
            StatementLanguage::Mongo,
            r#"{"collection":"Doc","filter":{}}"#,
        ))
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::LanguageDisabled);

    // Capabilities advertise only what is enabled (searchquery is always
    // implied; no statement language by default).
    let caps = executor
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Describe {
                describe: Describe::Capabilities,
            },
        })
        .unwrap();
    let Response::Capabilities { capabilities } = caps.response else {
        panic!("expected capabilities")
    };
    assert!(capabilities.contains(&"language:searchquery".to_string()));
    assert!(!capabilities.iter().any(|c| c.contains("language:mongo")));

    // Enable mongodb on a fresh node -> it runs end-to-end (find).
    let mut cfg2 = config(false);
    cfg2.languages = rushdb_protocol::LanguageSet::NONE;
    cfg2.languages.mongodb = true;
    let node2 = Node::bootstrap(&mut cfg2).unwrap();
    node2
        .scope()
        .create_record(
            "Doc",
            &serde_json::json!({ "text": "alpha" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let res = ProtocolExecutor::new(&node2, None)
        .execute(statement(
            StatementLanguage::Mongo,
            r#"{"collection":"Doc","filter":{"text":"alpha"}}"#,
        ))
        .unwrap();
    let Response::Rows { rows, .. } = res.response else {
        panic!("expected rows")
    };
    assert_eq!(rows.len(), 1);

    // The Mongo profile advertises unsupported aggregate families rather
    // than treating them as generic execution failures. Keep this at the
    // Protocol boundary where clients observe the typed error contract.
    let unsupported = ProtocolExecutor::new(&node2, None)
        .execute(statement(
            StatementLanguage::Mongo,
            r#"{"collection":"Doc","aggregate":[{"$lookup":{"from":"Other","as":"joined"}}]}"#,
        ))
        .unwrap_err();
    assert_eq!(unsupported.code, ErrorCode::Unsupported);

    // Syntax outside a published community-language profile has the same
    // typed boundary. It must not be exposed as an opaque execution fault.
    let mut cfg3 = config(false);
    cfg3.languages = rushdb_protocol::LanguageSet::NONE;
    cfg3.languages.gql = true;
    let node3 = Node::bootstrap(&mut cfg3).unwrap();
    let gql_unsupported = ProtocolExecutor::new(&node3, None)
        .execute(statement(
            StatementLanguage::Gql,
            "MATCH (n) WITH n ORDER BY n.id LIMIT 1 MATCH (m) RETURN m",
        ))
        .unwrap_err();
    assert_eq!(gql_unsupported.code, ErrorCode::Unsupported);
}

/// SQL index DDL (`CREATE VECTOR INDEX` / `DROP INDEX`) routes to the
/// native index operations instead of `DataFusion`.
#[test]
fn sql_index_ddl_creates_and_drops_vector_indexes() {
    use rushdb_protocol::StatementLanguage;

    let mut cfg = config(false);
    cfg.languages.sql = true;
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Doc",
            &serde_json::json!({ "text": "alpha" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    let executor = ProtocolExecutor::new(&node, None);
    let mutate_sql = |statement: &str| RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext::default(),
        operation: Operation::Mutate {
            mutation: Mutation::Statement {
                language: StatementLanguage::Sql,
                statement: statement.to_string(),
                parameters: None,
            },
        },
    };

    let created = executor
        .execute(mutate_sql(
            "CREATE VECTOR INDEX idx_doc ON Doc(text) SIMILARITY cosine DIMENSIONS 4",
        ))
        .unwrap();
    let Response::StatementMutation {
        status,
        affected: 1,
        ..
    } = created.response
    else {
        panic!("expected statement mutation")
    };
    let alias = status.expect("created index alias");
    assert_eq!(
        alias, "idx_doc",
        "SQL index names must remain durable identities"
    );

    let dropped = executor
        .execute(mutate_sql(&format!("DROP INDEX {alias}")))
        .unwrap();
    assert!(matches!(
        dropped.response,
        Response::StatementMutation { affected: 1, .. }
    ));
}

#[test]
fn delete_property_removes_occurrences_and_schema() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada", "nickname": "Countess" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Bob", "nickname": "B" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();

    // nickname appears in describe.properties before deletion.
    let props = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: None,
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Describe {
                describe: Describe::Properties {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();
    let Response::Properties { properties } = props.response else {
        panic!("expected properties response");
    };
    assert!(properties.iter().any(|p| p.name == "nickname"));

    // deleteProperty removes it atomically.
    let del = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: None,
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Mutate {
                mutation: rushdb_protocol::Mutation::DeleteProperty {
                    name: "nickname".into(),
                },
            },
        })
        .unwrap();
    let Response::Mutation { affected } = del.response else {
        panic!("expected mutation response");
    };
    assert_eq!(affected, 2, "two nickname occurrences removed");

    let after = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: None,
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Describe {
                describe: Describe::Properties {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();
    let Response::Properties { properties } = after.response else {
        panic!("expected properties response");
    };
    assert!(
        !properties.iter().any(|p| p.name == "nickname"),
        "deleted property must not appear in the schema"
    );
}

#[test]
fn protocol_scope_must_match_transport_scope() {
    let mut cfg = config(true);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let error = ProtocolExecutor::new(&node, Some("tenant-a".into()))
        .execute(join_request(Some("tenant-b")))
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::ScopeMismatch);
}

#[test]
fn policy_enforced_record_queries_intersect_labels_before_scanning() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let allowed_id = node
        .scope()
        .create_record(
            "Allowed",
            &serde_json::json!({"v": 1, "secret": "x"}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let denied_id = node
        .scope()
        .create_record(
            "Denied",
            &serde_json::json!({"v": 2}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    node.scope()
        .security_put_policy(
            &rushdb_authz::DataAccessPolicy {
                allowed_labels: vec!["Allowed".into()],
                visible_properties: std::collections::BTreeMap::from([(
                    "Allowed".into(),
                    vec!["v".into()],
                )]),
                ..Default::default()
            },
            rushdb_authz::AuthorizationMode::PolicyEnforced,
        )
        .unwrap();

    let response = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Records {
                    query: QueryDocument {
                        limit: Some(10),
                        ..Default::default()
                    },
                },
            },
        })
        .unwrap()
        .response;
    let Response::Records { records, total, .. } = response else {
        panic!("expected records response");
    };
    assert_eq!(total, 1);
    assert_eq!(records.len(), 1);
    assert!(records[0]["properties"].get("secret").is_none());

    let direct = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Record { id: allowed_id },
            },
        })
        .unwrap()
        .response;
    let Response::Record { record } = direct else {
        panic!("expected direct record response");
    };
    assert!(record["properties"].get("secret").is_none());

    let language_denied = ProtocolExecutor::new(&node, None)
        .execute(statement_request(
            StatementLanguage::Cypher,
            "MATCH (n) RETURN n",
        ))
        .unwrap_err();
    assert_eq!(language_denied.code, ErrorCode::Unauthorized);

    let denied = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::Record { id: denied_id },
            },
        })
        .unwrap_err();
    assert_eq!(denied.code, ErrorCode::Unauthorized);
}

#[test]
fn oversized_response_for_a_committed_mutation_is_not_retryable() {
    let mut cfg = config(false);
    // The response cap is the request-byte budget; keep it tiny so a normal
    // ingest response overflows it.
    cfg.max_request_bytes = 256;
    let node = Node::bootstrap(&mut cfg).unwrap();
    let error = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Ingest {
                ingest: Ingest::Documents {
                    label: Some("Big".into()),
                    documents: serde_json::json!([{ "payload": "x".repeat(4096) }]),
                    options: serde_json::json!({ "returnResult": true }),
                },
            },
        })
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::ResourceExhausted);
    assert!(
        !error.retryable,
        "a committed mutation must not be advertised as retryable"
    );
    assert!(
        node.committed_version() > 0,
        "the mutation must still be durable"
    );
}

#[test]
fn policy_enforced_reads_cannot_bypass_the_policy_gate() {
    use rushdb_protocol::{PropertyValuesMode, QueryDocument, VectorIndexCandidatesQuery};
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    for value in [1, 2] {
        node.scope()
            .create_record(
                "Allowed",
                &serde_json::json!({"v": value}),
                &[],
                &serde_json::json!({}),
            )
            .unwrap();
    }
    node.scope()
        .create_record(
            "Denied",
            &serde_json::json!({"v": 3}),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .security_put_policy(
            &rushdb_authz::DataAccessPolicy {
                allowed_labels: vec!["Allowed".into()],
                ..Default::default()
            },
            rushdb_authz::AuthorizationMode::PolicyEnforced,
        )
        .unwrap();

    // Operations that cannot intersect the policy must fail closed.
    let join = ProtocolExecutor::new(&node, None)
        .execute(join_request(None))
        .unwrap_err();
    assert_eq!(join.code, ErrorCode::Unauthorized, "join must be gated");

    let candidates = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::VectorIndexCandidates {
                    query: VectorIndexCandidatesQuery {
                        label: "Allowed".into(),
                        property: "v".into(),
                        index: "i".into(),
                        skip: 0,
                        limit: 10,
                    },
                },
            },
        })
        .unwrap_err();
    assert_eq!(
        candidates.code,
        ErrorCode::Unauthorized,
        "vector candidate enumeration must be gated"
    );

    // Property-value counts must be computed over the policy-intersected set,
    // not the global maintained postings (which would also see "Denied").
    let counts = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: Query::PropertyValues {
                    property: "v".into(),
                    query: QueryDocument::default(),
                    mode: Some(PropertyValuesMode::Count),
                    limit: None,
                    search: None,
                },
            },
        })
        .unwrap()
        .response;
    let Response::PropertyValues { distinct_count, .. } = counts else {
        panic!("expected property values response");
    };
    assert_eq!(
        distinct_count,
        Some(2),
        "policy must exclude the denied label's value"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
)]
fn purge_tenant_removes_all_scoped_data() {
    let mut cfg = config(true);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let scope = RequestScope::new(&node, Some("tenant-a".into()));
    scope
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada", "tags": ["a", "b"] }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    scope
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    scope
        .create_vector_index(&rushdb_protocol::VectorIndexDefinition {
            label: "Person".into(),
            property: "embedding".into(),
            dimensions: 2,
            similarity: "cosine".into(),
            name: Some("purge-owned-index".into()),
            model: None,
            owner: None,
        })
        .unwrap();
    let external_index_key = format!(
        "{}purge-owned-index",
        rushdb_types::keys::vector_index_meta_prefix(scope.tenant())
    );
    assert!(scope
        .lmpg()
        .store
        .read_view()
        .unwrap()
        .get(external_index_key.as_bytes())
        .unwrap()
        .is_some());

    let before = ProtocolExecutor::new(&node, Some("tenant-a".into()))
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: Some("tenant-a".into()),
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Describe {
                describe: Describe::Schema {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();
    let Response::Schema { schema } = before.response else {
        panic!("expected schema response")
    };
    assert_eq!(schema.labels.len(), 2);
    // per-label attribution: name (Person), tags (Person), name (Company)
    assert_eq!(schema.properties.len(), 3);

    let purged = ProtocolExecutor::new(&node, Some("tenant-a".into()))
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: Some("tenant-a".into()),
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::PurgeTenant,
        })
        .unwrap();
    assert!(matches!(purged.response, Response::PurgedTenant { .. }));

    // after purge the same scope reports no labels, properties or relationships
    let after = ProtocolExecutor::new(&node, Some("tenant-a".into()))
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: Some("tenant-a".into()),
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Describe {
                describe: Describe::Schema {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();
    let Response::Schema { schema } = after.response else {
        panic!("expected schema response")
    };
    assert!(schema.labels.is_empty(), "labels: {:?}", schema.labels);
    assert!(schema.relationships.is_empty());
    assert!(schema.properties.is_empty());
    assert!(
        scope
            .lmpg()
            .store
            .read_view()
            .unwrap()
            .get(external_index_key.as_bytes())
            .unwrap()
            .is_none(),
        "tenant-owned vector metadata must be purged too"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
)]
fn retype_relations_renames_edges_between_labels() {
    let mut cfg = config(false);
    let node = Node::bootstrap(&mut cfg).unwrap();
    node.scope()
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    node.scope()
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    // create the default edge directly through the LMPG (bypasses the
    // linkRecords join-key/where constraints)
    {
        let lmpg = node.scope().lmpg();
        let snap = lmpg.store.read_view().unwrap();
        let ords = test_access::label_members(&snap, lmpg.tenant, "Person");
        let src = test_access::record_by_ord(&snap, lmpg.tenant, ords[0]).unwrap();
        let ords = test_access::label_members(&snap, lmpg.tenant, "Company");
        let tgt = test_access::record_by_ord(&snap, lmpg.tenant, ords[0]).unwrap();
        lmpg.create_relationship(
            "__RUSHDB__RELATION__DEFAULT__",
            rushdb_lmpg::RecordRef {
                record_id: src.record_id,
                ordinal: src.ordinal,
            },
            rushdb_lmpg::RecordRef {
                record_id: tgt.record_id,
                ordinal: tgt.ordinal,
            },
            vec![],
        )
        .unwrap();
    }

    // confirm a default edge exists
    let q = rushdb_protocol::RelationshipQuery {
        relation_type: Some("__RUSHDB__RELATION__DEFAULT__".into()),
        ..Default::default()
    };
    let before = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: rushdb_protocol::Query::Relationships { query: q },
            },
        })
        .unwrap();
    let Response::Relationships { relationships, .. } = before.response else {
        panic!("expected relationships response")
    };
    assert_eq!(relationships.len(), 1, "default edge exists before retype");

    let retyped = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Mutate {
                mutation: rushdb_protocol::Mutation::RetypeRelations {
                    command: rushdb_protocol::RetypeRelationsMutation {
                        source_label: "Person".into(),
                        target_label: "Company".into(),
                        source_type: "__RUSHDB__RELATION__DEFAULT__".into(),
                        target_type: "FOUNDED".into(),
                        direction: rushdb_protocol::Direction::Out,
                    },
                },
            },
        })
        .unwrap();
    let Response::Mutation { affected } = retyped.response else {
        panic!("expected mutation response")
    };
    assert_eq!(affected, 1);

    // default edge gone, FOUNDED edge present
    let q_default = rushdb_protocol::RelationshipQuery {
        relation_type: Some("__RUSHDB__RELATION__DEFAULT__".into()),
        ..Default::default()
    };
    let after_default = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: rushdb_protocol::Query::Relationships { query: q_default },
            },
        })
        .unwrap();
    let Response::Relationships { relationships, .. } = after_default.response else {
        panic!("expected relationships response")
    };
    assert!(relationships.is_empty(), "old type removed");

    let q_founded = rushdb_protocol::RelationshipQuery {
        relation_type: Some("FOUNDED".into()),
        ..Default::default()
    };
    let after_founded = ProtocolExecutor::new(&node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Query {
                query: rushdb_protocol::Query::Relationships { query: q_founded },
            },
        })
        .unwrap();
    let Response::Relationships { relationships, .. } = after_founded.response else {
        panic!("expected relationships response")
    };
    assert_eq!(relationships.len(), 1, "new type created");
}

#[test]
fn backup_exports_consistent_snapshot_and_round_trips() {
    // multitenant: two scopes with data
    let mut cfg = config(true);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let scope = RequestScope::new(&node, Some("tenant-a".into()));
    scope
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada", "tags": ["a", "b"] }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    scope
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    // link the two records directly through the LMPG
    {
        let lmpg = scope.lmpg();
        let snap = lmpg.store.read_view().unwrap();
        let ps = test_access::label_members(&snap, lmpg.tenant, "Person");
        let cs = test_access::label_members(&snap, lmpg.tenant, "Company");
        let p = test_access::record_by_ord(&snap, lmpg.tenant, ps[0]).unwrap();
        let c = test_access::record_by_ord(&snap, lmpg.tenant, cs[0]).unwrap();
        lmpg.create_relationship(
            "FOUNDED",
            rushdb_lmpg::RecordRef {
                record_id: p.record_id,
                ordinal: p.ordinal,
            },
            rushdb_lmpg::RecordRef {
                record_id: c.record_id,
                ordinal: c.ordinal,
            },
            vec![(
                "since".into(),
                rushdb_lmpg::plan::FieldChange::Set(serde_json::json!(2020)),
            )],
        )
        .unwrap();
    }

    // take a backup pinned at the current commit index
    let response = ProtocolExecutor::new(&node, Some("tenant-a".into()))
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext {
                scope: Some("tenant-a".into()),
                deadline_ms: None,
                ..Default::default()
            },
            operation: Operation::Backup {
                request: rushdb_protocol::BackupRequest {
                    scopes: vec!["tenant-a".into()],
                },
            },
        })
        .unwrap();
    let Response::Backup {
        commit_index,
        tenants,
    } = response.response
    else {
        panic!("expected backup response")
    };
    assert_eq!(tenants.len(), 1);
    assert_eq!(tenants[0].scope.as_deref(), Some("tenant-a"));
    assert_eq!(tenants[0].records, 2);
    assert_eq!(tenants[0].relationships, 1);
    assert!(commit_index > 0);
    let jsonl = tenants[0].jsonl.clone();

    // restore into a fresh single-tenant node and compare canonical shape
    let mut cfg2 = config(false);
    let node2 = Node::bootstrap(&mut cfg2).unwrap();
    let (records, relationships) =
        rushdb_backup::restore_backup(&node2.scope().lmpg(), &jsonl).unwrap();
    assert_eq!(records, 2);
    assert_eq!(relationships, 1);

    let schema = ProtocolExecutor::new(&node2, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation: Operation::Describe {
                describe: Describe::Schema {
                    query: QueryDocument::default(),
                    detail: None,
                    skip: None,
                    limit: None,
                },
            },
        })
        .unwrap();
    let Response::Schema { schema } = schema.response else {
        panic!("expected schema response")
    };
    assert_eq!(schema.labels.len(), 2, "labels: {:?}", schema.labels);
    assert_eq!(schema.relationships.len(), 1);
    assert_eq!(schema.relationships[0].relationship_type, "FOUNDED");
    assert_eq!(schema.relationships[0].properties[0].name, "since");
}

#[test]
fn framed_snapshot_export_restores_and_rejects_corruption() {
    // Seed data in tenant-a (record + relationship, mirrors the plain
    // backup round-trip test).
    let mut cfg = config(true);
    let node = Node::bootstrap(&mut cfg).unwrap();
    let scope = RequestScope::new(&node, Some("tenant-a".into()));
    scope
        .create_record(
            "Person",
            &serde_json::json!({ "name": "Ada", "tags": ["a", "b"] }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    scope
        .create_record(
            "Company",
            &serde_json::json!({ "name": "RushDB" }),
            &[],
            &serde_json::json!({}),
        )
        .unwrap();
    {
        let lmpg = scope.lmpg();
        let snap = lmpg.store.read_view().unwrap();
        let ps = test_access::label_members(&snap, lmpg.tenant, "Person");
        let cs = test_access::label_members(&snap, lmpg.tenant, "Company");
        let p = test_access::record_by_ord(&snap, lmpg.tenant, ps[0]).unwrap();
        let c = test_access::record_by_ord(&snap, lmpg.tenant, cs[0]).unwrap();
        lmpg.create_relationship(
            "FOUNDED",
            rushdb_lmpg::RecordRef {
                record_id: p.record_id,
                ordinal: p.ordinal,
            },
            rushdb_lmpg::RecordRef {
                record_id: c.record_id,
                ordinal: c.ordinal,
            },
            vec![(
                "since".into(),
                rushdb_lmpg::plan::FieldChange::Set(serde_json::json!(2020)),
            )],
        )
        .unwrap();
    }

    // Export the framed, manifest-verified snapshot.

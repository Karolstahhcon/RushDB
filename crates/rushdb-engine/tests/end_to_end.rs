//! End-to-end tests for the in-process [`Engine`] facade.

use rushdb_engine::Engine;
use rushdb_engine::OperationClass;
use rushdb_lmpg::plan::{FieldChange, PlanError};
use rushdb_query::{Direction, Expr, FieldPred, Pagination, RelatedSpec, SearchRequest};
use rushdb_tx::Transaction;
use rushdb_types::command::IsolationMode;
use rushdb_types::ids::RecordId;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

const SCOPE: &str = "engine-test-scope";

#[test]
fn fallible_replay_rejects_malformed_log_before_publishing() {
    let Err(error) = Engine::try_open_recovered(&[b"not-json".to_vec()], Some(SCOPE)) else {
        panic!("malformed replay unexpectedly succeeded")
    };
    assert!(matches!(error, rushdb_storage::StorageError::CorruptLog));
}

#[test]
fn execution_context_pins_reads_and_enforces_response_budget() {
    let engine = Engine::new_in_memory();
    engine
        .import_document(Some(SCOPE), "Employee", &json!({"name": "Ada"}))
        .unwrap();
    let mut context = engine
        .context(Some(SCOPE), OperationClass::Read, None, Some(1))
        .unwrap();
    let request = SearchRequest {
        labels: vec!["Employee".into()],
        ..Default::default()
    };
    let error = engine
        .search_with_context(&mut context, &request)
        .unwrap_err();
    assert!(matches!(error, rushdb_query::QueryError::ResourceLimit(_)));

    let mut pinned = engine
        .context(Some(SCOPE), OperationClass::Read, None, None)
        .unwrap();
    engine
        .import_document(Some(SCOPE), "Employee", &json!({"name": "Grace"}))
        .unwrap();
    let page = engine.search_with_context(&mut pinned, &request).unwrap();
    assert_eq!(page.total_matched, 1);
}

#[test]
fn context_aware_import_honors_deadline_before_mutation() {
    let engine = Engine::new_in_memory();
    let mut context = engine
        .context(
            Some(SCOPE),
            OperationClass::Write,
            Some(std::time::Duration::ZERO),
            None,
        )
        .unwrap();
    let error = engine
        .import_document_with_context(&mut context, "Employee", &json!({"name": "Ada"}))
        .unwrap_err();
    assert!(error.to_string().contains("deadline"));
    let page = engine
        .search(
            Some(SCOPE),
            &SearchRequest {
                labels: vec!["Employee".into()],
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(page.total_matched, 0);
}

#[test]
fn context_aware_import_rejects_cancelled_and_foreign_contexts() {
    let source = Engine::new_in_memory();
    let destination = Engine::new_in_memory();
    let mut cancelled = source
        .context(None, OperationClass::Write, None, None)
        .unwrap();
    cancelled.read_view().cancellation().cancel();
    assert!(destination
        .import_document_with_context(&mut cancelled, "Audit", &json!({"value": 1}))
        .is_err());

    let mut foreign = source
        .context(None, OperationClass::Write, None, None)
        .unwrap();
    assert!(destination
        .import_document_with_context(&mut foreign, "Audit", &json!({"value": 1}))
        .is_err());
}

#[test]
fn context_aware_import_admits_staging_before_nested_allocations() {
    let engine = Engine::new_in_memory();
    let mut context = engine
        .context(None, OperationClass::Write, None, Some(8 * 1024))
        .unwrap();
    let nested = json!({"child": {"payload": "x".repeat(2048)}});
    assert!(engine
        .import_document_with_context(&mut context, "Audit", &nested)
        .is_err());
    let page = engine
        .search(
            None,
            &SearchRequest {
                labels: vec!["Audit".into()],
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(page.total_matched, 0);
}

#[test]
fn context_aware_import_allows_bounded_nested_documents() {
    let engine = Engine::new_in_memory();
    let mut context = engine
        .context(None, OperationClass::Write, None, Some(256 * 1024))
        .unwrap();
    let nested = json!({
        "child": {"payload": "x".repeat(256)}
    });
    engine
        .import_document_with_context(&mut context, "Audit", &nested)
        .unwrap();
    let page = engine
        .search(
            None,
            &SearchRequest {
                labels: vec!["Audit".into()],
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(page.total_matched, 1);
}

#[test]
fn rejected_import_restores_context_charge_for_reuse() {
    let engine = Engine::new_in_memory();
    let mut context = engine
        .context(None, OperationClass::Write, None, Some(32 * 1024))
        .unwrap();
    context.charge_bytes(1).unwrap();
    let before = context.charged_bytes();
    let rejected = json!({"child": {"payload": "x".repeat(16 * 1024)}});
    assert!(engine
        .import_document_with_context(&mut context, "Audit", &rejected)
        .is_err());
    assert_eq!(context.charged_bytes(), before);

    engine
        .import_document_with_context(&mut context, "Audit", &json!({"value": 1}))
        .unwrap();
    assert_eq!(context.charged_bytes(), before);
}

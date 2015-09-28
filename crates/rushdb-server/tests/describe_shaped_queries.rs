//! `Describe::Labels/Schema` must not 500 on output-shaping queries: label
//! discovery uses the raw matched-root search, so select/groupBy queries that
//! route through the aggregate path return Rows (no record label) instead of
//! panicking with "entered unreachable code".

use std::sync::Arc;

use rushdb_protocol::{Describe, EnvelopeHandler, Operation, RequestContext, RequestEnvelope};
use rushdb_server::{Node, NodeEnvelopeHandler, ServerConfig};

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single linear operation/reader; splitting it would scatter shared borrows and hurt auditability"
)]
fn describe_labels_and_schema_handle_shaped_queries() {
    let mut cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: "t".into(),
        data_dir: None,
        multitenant: false,
        capability_secret: None,
        raft: None,
        resources: rushdb_server::resource_config::ResourceConfig::default(),
        transactions: rushdb_server::resource_config::TransactionConfig::default(),
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
    };
    let node = Arc::new(Node::bootstrap(&mut cfg).expect("bootstrap"));
    let handler = NodeEnvelopeHandler::new(node.clone());

    // seed a small planet -> battle -> starship graph at the lmpg level
    {
        let lmpg = node.scope().lmpg();
        let p = rushdb_lmpg::import::import_document(
            &lmpg,
            "PLANET",
            &serde_json::json!({ "name": "Endor" }),
        )

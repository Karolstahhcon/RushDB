//! gRPC transport integration tests: real HTTP/2 wire round-trips through the
//! `Database.Execute` unary RPC, plus auth, tenant scope, error mapping, and
//! language gating.

use std::sync::Arc;

use rushdb_protocol::LanguageSet;
use rushdb_server::{Node, NodeEnvelopeHandler, ServerConfig};
use tonic::codec::ProstCodec;
use tonic::{transport::Channel, Request};

use rushdb_grpc::{BytesEnvelope, GrpcServer};

fn boot(token: &str, languages: LanguageSet) -> u16 {
    let mut cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: token.to_string(),
        data_dir: None,
        multitenant: false,
        capability_secret: None,
        raft: None,
        resources: rushdb_server::resource_config::ResourceConfig::default(),
        transactions: rushdb_server::resource_config::TransactionConfig::default(),
        languages,
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
    let handler: Arc<dyn rushdb_grpc::EnvelopeHandler> = Arc::new(NodeEnvelopeHandler::new(node));
    let (mut server, port) =
        GrpcServer::bind(handler, token.to_string(), "127.0.0.1", 0, 64 * 1024 * 1024)
            .expect("grpc bind");
    std::thread::spawn(move || server.serve_forever());
    port
}

async fn connect(port: u16, _token: &str) -> tonic::client::Grpc<Channel> {
    let channel = Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .expect("channel uri")
        .connect()
        .await
        .expect("connect");
    tonic::client::Grpc::new(channel)
}

async fn execute(
    grpc: &mut tonic::client::Grpc<Channel>,
    token: &str,
    envelope: serde_json::Value,
) -> Result<BytesEnvelope, tonic::Status> {
    let mut req = Request::new(BytesEnvelope {
        json: serde_json::to_vec(&envelope).expect("serialize envelope"),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("token metadata"),
    );
    let codec = ProstCodec::<BytesEnvelope, BytesEnvelope>::default();
    grpc.ready()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let path = tonic::codegen::http::uri::PathAndQuery::from_static("/rushdb.v1.Database/Execute");
    let resp = grpc.unary(req, path, codec).await?;
    Ok(resp.into_inner())
}

fn put_envelope(label: &str, id: &str) -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": 1,
        "operation": {
            "kind": "mutate",
            "mutation": {
                "kind": "putRecord",
                "mode": "create",
                "label": label,
                "properties": { "name": id },
                "options": {}
            }
        }
    })
}

fn query_envelope(label: &str) -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": 1,
        "operation": {
            "kind": "query",
            "query": {
                "kind": "records",
                "query": {
                    "labels": [label],
                    "predicate": {},
                    "offset": 0,
                    "limit": 100
                }
            }
        }
    })
}


//! 1A — the same contract fixture corpus must produce the identical protocol
//! `response` over HTTP and gRPC (both run the canonical executor). This is
//! the "same fixture corpus over HTTP and gRPC" acceptance.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use rushdb_protocol::LanguageSet;
use rushdb_server::{new_gateway, ServerConfig};
use tonic::codec::ProstCodec;
use tonic::transport::Channel;
use tonic::Request;

use rushdb_grpc::BytesEnvelope;
use std::sync::atomic::{AtomicU16, Ordering};

static GRPC_PORT: AtomicU16 = AtomicU16::new(39010);

fn boot(token: &str) -> (u16, u16, u16) {
    let grpc_port = GRPC_PORT.fetch_add(1, Ordering::Relaxed);
    let cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port,
        bolt_port: 0,
        token: token.to_string(),
        data_dir: None,
        multitenant: false,
        capability_secret: None,
        raft: None,
        resources: rushdb_server::resource_config::ResourceConfig::default(),
        transactions: rushdb_server::resource_config::TransactionConfig::default(),
        languages: LanguageSet {
            sql: true,
            gql: true,
            cypher: true,
            gremlin: true,
            sql_pgq: true,
            mongodb: true,
        },
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
    let (mut router, http_port, _pgwire, _, mut grpc, bound_grpc, _bolt, _) =
        new_gateway(cfg, 0).expect("gateway");
    std::thread::spawn(move || router.serve_forever());
    std::thread::spawn(move || grpc.serve_forever());
    (http_port, bound_grpc, bound_grpc)
}

fn http_execute(port: u16, token: &str, envelope: &serde_json::Value) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let payload = envelope.to_string();
    let request = format!(
        "POST /protocol/v1/execute HTTP/1.1\r\nHost: localhost\r\nX-RushDB-Token: {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).unwrap();
    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(0);
    let body = raw
        .split_once("\r\n\r\n")
        .and_then(|(_, v)| serde_json::from_str(v).ok())
        .unwrap_or(serde_json::Value::Null);
    (status, body)
}

async fn grpc_execute(
    port: u16,
    token: &str,
    envelope: &serde_json::Value,
) -> Result<serde_json::Value, tonic::Status> {
    let channel = Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .expect("channel")
        .connect()
        .await
        .expect("connect");
    let mut grpc = tonic::client::Grpc::new(channel);
    let mut req = Request::new(BytesEnvelope {
        json: serde_json::to_vec(envelope).expect("serialize"),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("meta"),
    );
    let codec = ProstCodec::<BytesEnvelope, BytesEnvelope>::default();
    grpc.ready()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let path = tonic::codegen::http::uri::PathAndQuery::from_static("/rushdb.v1.Database/Execute");
    let resp = grpc.unary(req, path, codec).await?;
    let json = serde_json::from_slice(&resp.into_inner().json).expect("response json");
    Ok(json)
}

/// Make a response comparable across transports: drop generated record ids and
/// sort arrays so record ordering does not mask an identical payload.
fn normalize_response(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(obj) => {
            let mut out = serde_json::Map::new();
            for (k, v) in obj {
                if k == "id" {
                    continue;
                }
                out.insert(k.clone(), normalize_response(v));
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => {
            let mut out: Vec<serde_json::Value> = items.iter().map(normalize_response).collect();
            out.sort_by_key(std::string::ToString::to_string);
            serde_json::Value::Array(out)
        }
        other => other.clone(),
    }
}

fn envelope(kind: &str, body: &serde_json::Value) -> serde_json::Value {
    let mut op = serde_json::Map::new();
    op.insert("kind".to_string(), serde_json::json!(kind));
    if let Some(obj) = body.as_object() {
        for (k, v) in obj {
            op.insert(k.clone(), v.clone());
        }
    }
    serde_json::json!({
        "protocolVersion": 1,
        "operation": op
    })
}


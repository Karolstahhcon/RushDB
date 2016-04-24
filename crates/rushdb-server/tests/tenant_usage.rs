//! USE-1: `POST /internal/usage` reports per-tenant usage for a list of
//! tenant scopes plus their total, takes the operator token, and never
//! registers a scope it is asked about.

use std::io::{Read, Write};
use std::sync::Arc;

use rushdb_protocol::VectorIndexDefinition;
use rushdb_server::{Node, RequestScope, ServerConfig};
use serde_json::{json, Value};

fn serve(node: &Arc<Node>, cfg: ServerConfig) -> u16 {
    let (mut router, port) = rushdb_server::http::bind_router(cfg, Arc::clone(node)).unwrap();
    std::thread::spawn(move || {
        let _ = router.serve_forever();
    });
    port
}

fn post(port: u16, token: &str, body: &str) -> (u16, Value) {
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        stream,
        "POST /internal/usage HTTP/1.1\r\nHost: x\r\nX-RushDB-Token: {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or_default();
    // Chunked bodies carry a size line before the JSON.
    let json_start = body.find('{').unwrap_or(0);
    let json_end = body.rfind('}').map_or(body.len(), |end| end + 1);
    (
        status,
        serde_json::from_str(&body[json_start..json_end]).unwrap(),
    )
}

fn seed(node: &Node, scope: &str, records: u16, with_vectors: bool) {
    let request = RequestScope::new(node, Some(scope.to_string()));
    if with_vectors {
        request
            .create_vector_index(&VectorIndexDefinition {
                name: Some("doc_embedding".into()),
                label: "DOC".into(),
                property: "embedding".into(),
                dimensions: 2,
                similarity: "cosine".into(),
                model: None,
                owner: None,
            })
            .unwrap();
    }
    let mut previous: Option<String> = None;
    for i in 0..records {
        let properties = json!({ "n": i, "name": format!("r{i}") });
        let created = request
            .create_record("DOC", &properties, &[], &json!({}))
            .unwrap();
        let id = created["id"].as_str().unwrap().to_string();
        if with_vectors {

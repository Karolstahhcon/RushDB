//! A protocol request whose HTTP client goes away is cancelled: the worker
//! running it stops at its next cancellation check instead of finishing work
//! nobody will read.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use rushdb_server::{new_router, ServerConfig};
use serde_json::{json, Value};

const RING: usize = 2_000;

fn start() -> u16 {
    let (mut router, port) = new_router(ServerConfig {
        port: 0,
        pgwire_port: 0,
        ..Default::default()
    })
    .unwrap();
    std::thread::spawn(move || router.serve_forever());
    port
}

fn send(port: u16, method: &str, path: &str, body: &str) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nhost: x\r\nauthorization: Bearer local-token\r\n\
         content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    stream
}

fn execute(port: u16, operation: &Value) -> Value {
    let body = json!({ "protocolVersion": 1, "context": {}, "operation": operation });
    let mut stream = send(port, "POST", "/protocol/v1/execute", &body.to_string());
    let mut raw = String::new();
    stream.read_to_string(&mut raw).unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{raw}");
    serde_json::from_str(body).unwrap()

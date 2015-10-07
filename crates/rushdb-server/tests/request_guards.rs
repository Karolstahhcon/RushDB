//! C7 (cloud-capacity F8): request bodies are not read before the caller is
//! authenticated, an oversized declared body is refused up front, and the
//! node-wide in-flight body budget sheds load instead of buffering it.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use rushdb_server::http::new_router;
use rushdb_server::ServerConfig;

fn start(cfg: ServerConfig) -> u16 {
    let (mut router, port) = new_router(cfg).unwrap();
    std::thread::spawn(move || router.serve_forever());
    port
}

/// Send only the request head (declaring `content_length`), never the body,
/// and return the status line the server answers with.
fn head_only(port: u16, token: &str, content_length: usize) -> (TcpStream, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        stream,
        "POST /protocol/v1/execute HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {token}\r\n\
         content-type: application/json\r\ncontent-length: {content_length}\r\n\r\n"
    )
    .unwrap();
    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).unwrap_or(0);

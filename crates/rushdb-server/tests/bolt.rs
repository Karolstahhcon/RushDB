//! Bolt (Neo4j wire) transport integration tests. A hand-rolled Bolt client
//! drives a real `BoltServer` over TCP: handshake, HELLO auth, RUN/PULL for
//! read + write Cypher, and BEGIN/COMMIT/ROLLBACK cross-service transactions.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Number, Value};

use rushdb_bolt::value;
use rushdb_bolt::BoltServer;
use rushdb_protocol::EnvelopeHandler;
use rushdb_server::{Node, NodeEnvelopeHandler, ServerConfig};

const MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const VERSION_5_0: [u8; 4] = [0x00, 0x00, 0x00, 0x05];
const SUCCESS: u8 = 0x70;
const RECORD: u8 = 0x71;
const IGNORED: u8 = 0x7E;
const FAILURE: u8 = 0x7F;

fn boot(token: &str, cypher: bool) -> u16 {
    boot_with(token, cypher, false)
}

fn boot_with(token: &str, cypher: bool, multitenant: bool) -> u16 {
    let mut cfg = ServerConfig {
        port: 0,
        pgwire_port: 0,
        grpc_port: 0,
        bolt_port: 0,
        token: token.to_string(),
        data_dir: None,
        multitenant,
        capability_secret: None,
        raft: None,
        resources: rushdb_server::resource_config::ResourceConfig::default(),
        transactions: rushdb_server::resource_config::TransactionConfig::default(),
        languages: rushdb_protocol::LanguageSet {
            cypher,
            ..Default::default()
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
    let node = Arc::new(Node::bootstrap(&mut cfg).expect("bootstrap"));
    let handler: Arc<dyn EnvelopeHandler> = Arc::new(NodeEnvelopeHandler::new(node));
    let (mut server, port) =
        BoltServer::bind(handler, token.to_string(), "127.0.0.1", 0).expect("bolt bind");
    std::thread::spawn(move || server.serve_forever());
    port
}

struct Client {
    stream: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Client {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        // Handshake: magic + 4 Bolt 5.0 proposals.
        let mut handshake = Vec::new();
        handshake.extend_from_slice(&MAGIC);
        for _ in 0..4 {
            handshake.extend_from_slice(&VERSION_5_0);
        }
        stream.write_all(&handshake).expect("write handshake");
        let mut version = [0u8; 4];
        stream.read_exact(&mut version).expect("read version");
        assert_eq!(version, VERSION_5_0, "negotiated Bolt 5.0");
        Client { stream }
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "the chunk length is bounded by the chunker (<= 65535), so the usize-to-u16 conversion cannot truncate"
    )]
    fn send(&mut self, message: &[u8]) {
        let mut offset = 0;
        while offset < message.len() {
            let chunk = (message.len() - offset).min(65_535);
            self.stream
                .write_all(&(chunk as u16).to_be_bytes())
                .expect("write chunk header");
            self.stream
                .write_all(&message[offset..offset + chunk])
                .expect("write chunk");
            offset += chunk;
        }
        self.stream.write_all(&[0, 0]).expect("write terminator");
    }

    fn recv(&mut self) -> Value {
        let mut payload = Vec::new();
        loop {
            let mut len = [0u8; 2];
            self.stream.read_exact(&mut len).expect("read chunk header");

            let chunk = u16::from_be_bytes(len) as usize;
            if chunk == 0 {
                return value::decode(&payload).expect("decode message");
            }
            let start = payload.len();
            payload.resize(start + chunk, 0);
            self.stream
                .read_exact(&mut payload[start..])
                .expect("read chunk");
        }
    }

    fn call(&mut self, tag: u8, fields: Vec<Value>) -> Value {
        let mut out = Vec::new();
        value::encode_struct(tag, fields, &mut out);
        self.send(&out);
        self.recv()
    }

    fn hello(&mut self, token: &str) -> Value {
        self.hello_db(token, None)
    }

    fn hello_db(&mut self, token: &str, db: Option<&str>) -> Value {
        let mut meta = Map::new();
        if let Some(db) = db {
            meta.insert("db".into(), Value::String(db.into()));
        }
        meta.insert("scheme".into(), Value::String("basic".into()));
        meta.insert("principal".into(), Value::String(token.into()));
        meta.insert("credentials".into(), Value::String(token.into()));
        meta.insert("user_agent".into(), Value::String("rust-bolt-test".into()));
        self.call(0x01, vec![Value::Object(meta)])
    }

    fn run(&mut self, statement: &str) -> Value {
        let params = Map::new();
        self.call(
            0x10,
            vec![Value::String(statement.into()), Value::Object(params)],
        )
    }

    fn pull(&mut self) -> Vec<Value> {
        let mut meta = Map::new();

//! Bolt (Neo4j binary wire protocol) transport for the canonical protocol.
//!
//! A hand-rolled Bolt v5 server that maps Bolt messages onto the canonical
//! envelopes and executes them through `rushdb_protocol::EnvelopeHandler`
//! (the same surface gRPC and HTTP use). Cypher statements run through the
//! existing language layer — the engine already executes openCypher — so this
//! crate is a protocol translator, not a query engine.
//!
//! Supported: HELLO (basic auth), RUN/PULL (read + write Cypher), BEGIN /
//! COMMIT / ROLLBACK (cross-service transactions), RESET / DISCARD, ROUTE,
//! GOODBYE.

pub mod value;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Number, Value};

use rushdb_protocol::{
    EnvelopeHandler, ErrorCode, LanguageSet, Operation, ProtocolError, Query, RequestContext,
    RequestEnvelope, StatementLanguage, TransactionMutation, TransactionOperation,
};

/// Magic bytes that begin every Bolt handshake.
const MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
/// The Bolt version we negotiate (Bolt 5.0).
const VERSION_5_0: [u8; 4] = [0x00, 0x00, 0x00, 0x05];

// Bolt struct tags.
const HELLO: u8 = 0x01;
const GOODBYE: u8 = 0x02;
const RESET: u8 = 0x0F;
const RUN: u8 = 0x10;
const BEGIN: u8 = 0x11;
const COMMIT: u8 = 0x12;
const ROLLBACK: u8 = 0x13;
const DISCARD: u8 = 0x2F;
const PULL: u8 = 0x3F;
const ROUTE: u8 = 0x42;
const SUCCESS: u8 = 0x70;
const RECORD: u8 = 0x71;
const IGNORED: u8 = 0x7E;
const FAILURE: u8 = 0x7F;

const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const MAX_PENDING_ROWS: usize = 100_000;
const MAX_CONNECTIONS: usize = 1024;
const CONNECTION_IO_TIMEOUT: Duration = Duration::from_secs(60);
const AUTH_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
static ACTIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

struct ConnectionPermit;

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    }
}

fn acquire_connection() -> Result<ConnectionPermit, String> {
    ACTIVE_CONNECTIONS
        .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |active| {
            (active < MAX_CONNECTIONS).then_some(active + 1)
        })
        .map(|_| ConnectionPermit)
        .map_err(|_| "bolt: connection limit reached".into())
}

/// Classifies a Cypher statement as a write exactly like Grafeo does
/// (`query_looks_like_mutation`), so read and write intents route to the
/// `Query::Statement` / `Mutation::Statement` envelopes respectively.
#[must_use]
pub fn looks_like_mutation(statement: &str) -> bool {
    // Invalid/unsupported syntax is sent through the mutation envelope so
    // the canonical frontend returns its structured parser error rather than
    // accidentally treating an unknown write as a read.
    rushdb_language::classify_cypher_mutation(statement).unwrap_or(true)
}

// ---- framing --------------------------------------------------------------

/// Read one full (de-chunked) Bolt message from the stream.
fn read_message(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut payload = Vec::new();
    loop {
        let mut len = [0u8; 2];
        stream
            .read_exact(&mut len)
            .map_err(|e| format!("bolt: read chunk header: {e}"))?;
        let chunk_len = u16::from_be_bytes(len) as usize;
        if chunk_len == 0 {
            // Chunk terminator: message complete.
            if payload.is_empty() {
                return Err("bolt: empty message".into());
            }
            return Ok(payload);
        }
        if payload.len() + chunk_len > MAX_MESSAGE_BYTES {
            return Err("bolt: message too large".into());
        }
        let start = payload.len();
        payload.resize(start + chunk_len, 0);
        stream
            .read_exact(&mut payload[start..])
            .map_err(|e| format!("bolt: read chunk: {e}"))?;
    }
}

/// Write one Bolt message with chunk framing.
fn write_message(stream: &mut TcpStream, payload: &[u8]) -> Result<(), String> {
    let mut offset = 0;
    while offset < payload.len() {
        let chunk = payload.len() - offset;
        let chunk = chunk.min(usize::from(u16::MAX));
        let len = u16::try_from(chunk)
            .expect("chunk is clamped to u16::MAX")
            .to_be_bytes();
        stream.write_all(&len).map_err(|e| e.to_string())?;
        stream
            .write_all(&payload[offset..offset + chunk])
            .map_err(|e| e.to_string())?;
        offset += chunk;
    }
    stream.write_all(&[0x00, 0x00]).map_err(|e| e.to_string())
}

// ---- responses ------------------------------------------------------------

fn success(metadata: Map<String, Value>) -> Vec<u8> {
    let mut out = Vec::new();
    value::encode_struct(SUCCESS, vec![Value::Object(metadata)], &mut out);
    out
}

fn ignored() -> Vec<u8> {
    let mut out = Vec::new();
    value::encode_struct(IGNORED, Vec::new(), &mut out);
    out
}

/// Whether an encoded response is a FAILURE: a one-field struct is the
/// tiny-struct marker followed by its tag.
fn is_failure(message: &[u8]) -> bool {
    message.get(1) == Some(&FAILURE)
}

fn failure(code: &str, message: &str) -> Vec<u8> {
    let mut meta = Map::new();
    meta.insert("code".into(), Value::String(code.to_string()));
    meta.insert("message".into(), Value::String(message.to_string()));
    let mut out = Vec::new();
    value::encode_struct(FAILURE, vec![Value::Object(meta)], &mut out);
    out
}

fn record(values: Vec<Value>) -> Vec<u8> {
    let mut out = Vec::new();
    value::encode_struct(RECORD, vec![Value::Array(values)], &mut out);
    out
}

/// Neo4j-style error code for a protocol error.
fn neo_code(error: &ProtocolError) -> &'static str {
    // Shared error taxonomy: the same ErrorCode maps to the same Neo4j-style
    // status code on every adapter. Add below — do not reorder.
    match error.code {
        ErrorCode::UnsupportedVersion => "Neo.ClientError.Request.Invalid",
        ErrorCode::ProtocolViolation
        | ErrorCode::InvalidRequest
        | ErrorCode::LanguageDisabled
        | ErrorCode::Unsupported => "Neo.ClientError.Statement.SyntaxError",
        ErrorCode::Unauthorized | ErrorCode::MissingScope | ErrorCode::ScopeMismatch => {
            "Neo.ClientError.Security.Unauthorized"
        }
        ErrorCode::NotFound => "Neo.ClientError.Statement.EntityNotFound",
        ErrorCode::Conflict => "Neo.ClientError.Constraint.UniqueConstraintViolation",
        ErrorCode::WriteConflict => "Neo.TransientError.Transaction.DeadlockDetected",
        ErrorCode::Cancelled => "Neo.ClientError.Transaction.TransactionTerminated",
        ErrorCode::ResourceExhausted | ErrorCode::Unavailable => {
            "Neo.TransientError.General.DatabaseUnavailable"
        }
        ErrorCode::ExecutionFailed => "Neo.DatabaseError.Execution.Failed",
    }
}

// ---- per-connection state machine ------------------------------------------

struct Connection {
    stream: TcpStream,
    handler: Arc<dyn EnvelopeHandler>,
    token: String,
    /// HELLO authentication must succeed before any operation is dispatched.
    authenticated: bool,
    /// Multitenant scope selected by the HELLO `db` field.
    scope: Option<String>,
    /// Pending RUN result (buffered rows + column order).
    pending: Option<PendingResult>,
    /// Cross-service transaction opened by BEGIN.
    transaction_id: Option<String>,
    /// Set by a FAILURE: until RESET, every message but GOODBYE is IGNORED.
    failed: bool,
    /// Server identity used in routing metadata.
    address: String,
}

struct PendingResult {
    fields: Vec<String>,
    rows: Vec<Map<String, Value>>,
}

enum MessageResponse {
    Buffered(Vec<Vec<u8>>),
    StreamPull(Vec<Value>),
}

fn pull_window(fields: &[Value]) -> Result<usize, String> {
    if fields.len() > 1 {
        return Err("bolt: PULL/DISCARD accepts at most one metadata map".to_string());
    }
    let Some(first) = fields.first() else {
        return Ok(usize::MAX);
    };
    let Some(meta) = first.as_object() else {
        return Err("bolt: PULL/DISCARD metadata must be a map".to_string());
    };
    let Some(raw) = meta.get("n") else {
        return Ok(usize::MAX);
    };
    let n = raw
        .as_i64()
        .ok_or_else(|| "bolt: PULL/DISCARD `n` must be an integer".to_string())?;
    // Bolt uses -1 to request all remaining rows. Other negative windows are
    // malformed and must fail closed.
    if n == -1 {
        return Ok(usize::MAX);
    }
    let n = usize::try_from(n)
        .map_err(|_| "bolt: PULL/DISCARD `n` must be non-negative or -1".to_string())?;
    Ok(n)
}

fn discard_pending(pending: &mut PendingResult, window: usize) -> bool {
    let count = window.min(pending.rows.len());
    pending.rows.drain(..count);
    !pending.rows.is_empty()
}

impl Connection {
    /// Every envelope carries the HELLO-selected scope in its context too: the
    /// executor requires the envelope scope to match the transport scope.
    fn execute(&self, mut envelope: RequestEnvelope) -> Result<Value, ProtocolError> {
        envelope.context.scope.clone_from(&self.scope);
        let response = self.handler.execute(envelope, self.scope.clone())?;
        serde_json::to_value(response.response)
            .map_err(|e| ProtocolError::new(ErrorCode::ExecutionFailed, e.to_string()))
    }

    fn handle_decoded(&mut self, decoded: &Value) -> Result<MessageResponse, String> {
        let tag = value::struct_tag(decoded).ok_or("bolt: message is not a struct")?;
        let fields = value::struct_fields(decoded);
        if tag != HELLO && tag != GOODBYE && !self.authenticated {
            return Ok(MessageResponse::Buffered(vec![failure(
                "Neo.ClientError.Security.Unauthorized",
                "HELLO authentication is required before this operation.",
            )]));
        }
        if self.failed && tag != RESET && tag != GOODBYE {
            return Ok(MessageResponse::Buffered(vec![ignored()]));
        }
        match tag {
            HELLO => Ok(MessageResponse::Buffered(self.handle_hello(&fields))),
            RUN => self.handle_run(&fields).map(MessageResponse::Buffered),
            PULL => Ok(MessageResponse::StreamPull(fields)),
            BEGIN => self.handle_begin(&fields).map(MessageResponse::Buffered),
            COMMIT => Ok(MessageResponse::Buffered(self.handle_commit())),
            ROLLBACK => Ok(MessageResponse::Buffered(self.handle_rollback())),
            RESET => {
                // RESET clears a failure and ends any open transaction, so a
                // later RUN auto-commits instead of staging into an abandoned one.
                self.failed = false;
                self.pending = None;
                if let Some(tx_id) = self.transaction_id.take() {
                    if let Err(error) = self.rollback(tx_id) {
                        return Ok(MessageResponse::Buffered(vec![failure(
                            neo_code(&error),
                            &error.message,
                        )]));
                    }
                }
                Ok(MessageResponse::Buffered(vec![success(Map::new())]))
            }
            DISCARD => Ok(MessageResponse::Buffered(self.handle_discard(&fields))),
            ROUTE => Ok(MessageResponse::Buffered(self.handle_route(&fields))),
            GOODBYE => Ok(MessageResponse::Buffered(Vec::new())),
            other => Err(format!("bolt: unsupported message tag 0x{other:02x}")),
        }
    }

    fn handle_hello(&mut self, fields: &[Value]) -> Vec<Vec<u8>> {
        let meta = fields
            .first()
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let scheme = meta
            .get("scheme")
            .and_then(Value::as_str)
            .unwrap_or("basic");
        let credentials = meta
            .get("credentials")
            .and_then(Value::as_str)
            .unwrap_or("");
        let authorized = match scheme {
            "none" => self.token.is_empty(),
            _ => rushdb_protocol::constant_time_eq(credentials.as_bytes(), self.token.as_bytes()),
        };
        if !authorized {
            self.authenticated = false;
            return vec![failure(
                "Neo.ClientError.Security.Unauthorized",
                "The client is unauthorized due to authentication failure.",
            )];
        }
        self.authenticated = true;
        // The `db` field selects the tenant scope (multitenant mode); a
        // single-tenant server refuses statements sent under one.
        if let Some(db) = meta.get("db").and_then(Value::as_str) {
            if !db.is_empty() && !db.eq_ignore_ascii_case("neo4j") {
                self.scope = Some(db.to_string());
            }
        }
        let mut resp = Map::new();
        resp.insert("server".into(), Value::String("RushDB/1.0".into()));
        resp.insert("connection_id".into(), Value::String("bolt-1".into()));
        let hints = Map::new();
        resp.insert("hints".into(), Value::Object(hints));
        vec![success(resp)]
    }

    fn run_envelope(
        &self,
        statement: &str,
        parameters: Option<Value>,
    ) -> Result<Value, ProtocolError> {
        let language = StatementLanguage::Cypher;
        let mutation = looks_like_mutation(statement);
        // Inside an open transaction a write is staged; its exact delta commits
        // or rolls back with the transaction. A read runs against committed
        // data, as reads in a transaction do on every other transport.
        if let (Some(tx_id), true) = (&self.transaction_id, mutation) {
            let envelope = RequestEnvelope {
                protocol_version: 1,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Transaction {
                    transaction: TransactionOperation::Stage {
                        transaction_id: tx_id.clone(),
                        mutation: TransactionMutation::Statement {
                            language,
                            statement: statement.to_string(),
                            parameters,
                        },
                    },
                },
            };
            return self.execute(envelope);
        }
        if mutation {
            let envelope = RequestEnvelope {
                protocol_version: 1,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Mutate {
                    mutation: rushdb_protocol::Mutation::Statement {
                        language,
                        statement: statement.to_string(),
                        parameters,
                    },
                },
            };
            self.execute(envelope)
        } else {
            let envelope = RequestEnvelope {
                protocol_version: 1,
                request_id: None,
                context: RequestContext::default(),
                operation: Operation::Query {
                    query: Query::Statement {
                        language,
                        statement: statement.to_string(),
                        parameters,
                    },
                },
            };
            self.execute(envelope)
        }
    }

    fn handle_run(&mut self, fields: &[Value]) -> Result<Vec<Vec<u8>>, String> {
        let statement = fields
            .first()
            .and_then(Value::as_str)
            .ok_or("bolt: RUN needs a statement string")?
            .to_string();
        let parameters = fields.get(1).cloned();
        let result = self.run_envelope(&statement, parameters);
        match result {
            Ok(response) => {
                let rows = response
                    .get("rows")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if rows.len() > MAX_PENDING_ROWS {
                    return Ok(vec![failure(
                        "Neo.TransientError.General.DatabaseUnavailable",
                        "RUN result exceeds the bounded Bolt pending-result limit; use a smaller query page.",
                    )]);
                }
                let mut row_maps = Vec::with_capacity(rows.len());
                let mut fields_order: Vec<String> = Vec::new();
                for row in rows {
                    let obj = row.as_object().cloned().unwrap_or_default();
                    if fields_order.is_empty() {
                        fields_order = obj.keys().cloned().collect();
                    }
                    row_maps.push(obj);
                }
                let mut meta = Map::new();
                meta.insert(
                    "fields".into(),
                    Value::Array(
                        fields_order
                            .iter()
                            .map(|f| Value::String(f.clone()))
                            .collect(),
                    ),
                );
                let type_name = if looks_like_mutation(&statement) {
                    "w"
                } else {
                    "r"
                };
                meta.insert("type".into(), Value::String(type_name.into()));
                self.pending = Some(PendingResult {
                    fields: fields_order,
                    rows: row_maps,
                });

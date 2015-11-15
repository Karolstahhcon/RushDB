//! gRPC transport for the canonical rush-rushdb protocol.
//!
//! A single unary `Execute` RPC carries the canonical `RequestEnvelope` /
//! `ResponseEnvelope` as a JSON payload inside a `bytes` protobuf field. Every
//! operation (query/mutate/describe/index/ingest/transaction/purge/backup) goes
//! through the same protocol surface as HTTP — no new execution logic.
//!
//! Auth: `authorization: Bearer <token>` metadata (the boot `--token` secret).
//! Tenant scope: `x-rushdb-tenant` metadata (multitenant mode).
//!
//! The crate is transport-only: it knows the canonical envelope types and a
//! handler trait, never the engine internals (no dependency on the server
//! crate, so the server can bind it without a dependency cycle).

use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tonic::{codec::ProstCodec, server::Grpc, Request, Response, Status};

/// Upper bound on concurrently executing requests (and HTTP/2 streams) per
/// gRPC connection (C7, cloud-capacity F8).
const MAX_CONCURRENT_REQUESTS_PER_CONNECTION: usize = 64;
/// HTTP/2 stream cap per gRPC connection; equal to the request cap above.
const MAX_CONCURRENT_STREAMS: u32 = 64;

pub use rushdb_protocol::EnvelopeHandler;
use rushdb_protocol::RequestEnvelope;

/// Protobuf wire envelope: field 1 = the canonical JSON envelope bytes.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BytesEnvelope {
    /// Canonical JSON-encoded request or response envelope.
    pub json: Vec<u8>,
}

impl prost::Message for BytesEnvelope {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        prost::encoding::bytes::encode(1, &self.json, buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        if tag == 1 {
            prost::encoding::bytes::merge(wire_type, &mut self.json, buf, ctx)
        } else {
            prost::encoding::skip_field(wire_type, tag, buf, ctx)
        }
    }

    fn encoded_len(&self) -> usize {
        prost::encoding::bytes::encoded_len(1, &self.json)
    }

    fn clear(&mut self) {
        self.json.clear();
    }
}

/// The gRPC `Database.Execute` unary handler.
#[derive(Clone)]
pub struct DatabaseService {
    handler: Arc<dyn EnvelopeHandler>,
    token: String,
}

impl DatabaseService {
    /// Creates a service that authenticates `token` and dispatches request
    /// envelopes to `handler`.
    #[must_use]
    pub fn new(handler: Arc<dyn EnvelopeHandler>, token: String) -> Self {
        DatabaseService { handler, token }
    }

    fn execute(&self, request: Request<BytesEnvelope>) -> Result<Response<BytesEnvelope>, Status> {
        let metadata = request.metadata();
        let token = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        if !rushdb_protocol::constant_time_eq(token.as_bytes(), self.token.as_bytes()) {
            return Err(Status::unauthenticated("invalid token"));
        }
        let scope = metadata
            .get("x-rushdb-tenant")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let payload = request.into_inner().json;
        let envelope: RequestEnvelope = serde_json::from_slice(&payload)
            .map_err(|e| Status::invalid_argument(format!("invalid envelope: {e}")))?;

        let result = self.handler.execute(envelope, scope);
        let response = result.map_err(|error| grpc_status(&error))?;
        let encoded = serde_json::to_vec(&response)
            .map_err(|e| Status::internal(format!("serialize response: {e}")))?;
        Ok(Response::new(BytesEnvelope { json: encoded }))
    }
}

/// tower service adapter: `Grpc::unary` accepts any
/// `Service<Request<BytesEnvelope>>` (blanket `UnaryService` impl).
impl tower::Service<Request<BytesEnvelope>> for DatabaseService {
    type Response = Response<BytesEnvelope>;
    type Error = Status;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<BytesEnvelope>) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || this.execute(request))
                .await
                .map_err(|error| Status::internal(format!("gRPC execution task failed: {error}")))?
        })
    }
}

/// The gRPC routing service bound by `tonic::transport::Server`.
pub struct DatabaseGrpc<S> {
    inner: S,
    max_message_size: usize,
}

impl<S> Clone for DatabaseGrpc<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        DatabaseGrpc {
            inner: self.inner.clone(),
            max_message_size: self.max_message_size,
        }
    }
}

impl<S> tonic::server::NamedService for DatabaseGrpc<S> {
    const NAME: &'static str = "rushdb.v1.Database";
}

impl<S> tower::Service<http::Request<tonic::body::BoxBody>> for DatabaseGrpc<S>
where
    S: tonic::server::UnaryService<BytesEnvelope, Response = BytesEnvelope>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let path = request.uri().path().to_string();
        let inner = self.inner.clone();
        let max_message_size = self.max_message_size;
        Box::pin(async move {
            if path == "/rushdb.v1.Database/Execute" {
                // Match the HTTP transport's body budget (configurable,
                // default 64MB): tonic's default per-message limit is 4MB,
                // which would reject large batch imports that are fine over HTTP.
                let mut grpc = Grpc::new(ProstCodec::default())
                    .max_decoding_message_size(max_message_size)
                    .max_encoding_message_size(max_message_size);
                Ok(grpc.unary(inner, request).await)
            } else {
                let mut not_found = http::Response::new(tonic::body::boxed(
                    http_body_util::Empty::<bytes::Bytes>::new(),
                ));
                *not_found.status_mut() = http::StatusCode::NOT_FOUND;
                Ok(not_found)
            }
        })
    }
}

/// gRPC server mirroring the pgwire adapter: a non-binding disabled variant and
/// a pre-bound listener served from a dedicated runtime thread.
pub struct GrpcServer {
    handler: Option<Arc<dyn EnvelopeHandler>>,
    token: Option<String>,
    listener: Option<std::net::TcpListener>,
    max_message_size: usize,
}

impl GrpcServer {
    /// A server that does not bind (no grpc-port configured).
    #[must_use]
    pub fn disabled() -> Self {
        GrpcServer {
            handler: None,
            token: None,
            listener: None,
            max_message_size: 64 * 1024 * 1024,
        }
    }

    /// Pre-binds the listener and returns the server plus the bound port.
    /// `max_message_size` matches the HTTP transport's body budget so large
    /// batch imports work over gRPC too (tonic's default per-message limit is
    /// 4MB).
    ///
    /// # Errors
    ///
    /// Returns an error string if the listen address cannot be bound, its
    /// local address cannot be read, or the listener cannot be set
    /// non-blocking.
    pub fn bind(
        handler: Arc<dyn EnvelopeHandler>,
        token: String,
        listen_addr: &str,
        port: u16,
        max_message_size: usize,
    ) -> Result<(Self, u16), String> {
        let l = std::net::TcpListener::bind((listen_addr, port)).map_err(|e| e.to_string())?;
        let p = l.local_addr().map_err(|e| e.to_string())?.port();
        l.set_nonblocking(true).map_err(|e| e.to_string())?;
        Ok((
            GrpcServer {
                handler: Some(handler),
                token: Some(token),
                listener: Some(l),
                max_message_size,
            },
            p,
        ))
    }

    /// Serves the pre-bound listener until shutdown on the caller-owned
    /// transport thread.
    ///
    /// # Panics
    ///
    /// Panics if a listener is present without a handler or token (only
    /// possible when not constructed via [`GrpcServer::bind`]), or if the
    /// tokio runtime, listener conversion, or server fails to start.
    /// # Errors
    /// Returns the listener failure or a bounded-drain timeout.
    pub fn serve_forever(&mut self) -> Result<(), String> {
        let Some(l) = self.listener.take() else {
            // grpc-port not configured: nothing to serve.
            return Ok(());
        };
        let handler = self.handler.take().unwrap();
        let token = self.token.take().unwrap();
        let max_message_size = self.max_message_size;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("grpc runtime");
        let outcome = rt.block_on(async move {
            let service = DatabaseService::new(handler, token);
            let shutdown_handler = Arc::clone(&service.handler);
            let handler_for_deadline = Arc::clone(&service.handler);
            let router = DatabaseGrpc {
                inner: service,
                max_message_size,
            };
            let stream = tokio_stream::wrappers::TcpListenerStream::new(
                tokio::net::TcpListener::from_std(l).expect("grpc listener"),
            );
            // The per-message budget is enforced by the codec above
            // (`max_decoding/encoding_message_size`); HTTP/2 frame size
            // stays at the default (tonic enforces h2's 16MB ceiling).

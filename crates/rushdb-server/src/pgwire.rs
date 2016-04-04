//! PostgreSQL adapter backed by the standard `pgwire` crate.
use crate::{http::HttpRouter, node::Node, RequestScope, ServerConfig};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures::stream;
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;
use rushdb_protocol::{
    ErrorCode, Operation, ProtocolError, Query, RequestContext, RequestEnvelope,
    Response as RushResponse, PROTOCOL_VERSION,
};
use serde_json::{Map, Value};
use std::{fmt::Debug, net::TcpListener, sync::Arc};

mod settings;

/// A bound PostgreSQL wire-protocol adapter, or a disabled placeholder when the
/// `sql` statement language is off.
pub struct PgWireServer {
    cfg: Option<ServerConfig>,
    node: Option<Arc<Node>>,
    listener: Option<TcpListener>,
}
impl PgWireServer {
    /// A server that does not bind (the `sql` language is disabled at startup).
    #[must_use]
    pub fn disabled() -> Self {
        PgWireServer {
            cfg: None,
            node: None,
            listener: None,
        }
    }

    fn bind(cfg: ServerConfig, node: Arc<Node>, port: u16) -> Result<(Self, u16), String> {
        let l = TcpListener::bind((cfg.listen_addr.as_str(), port)).map_err(|e| e.to_string())?;
        let p = l.local_addr().map_err(|e| e.to_string())?.port();
        l.set_nonblocking(true).map_err(|e| e.to_string())?;
        Ok((
            Self {
                cfg: Some(cfg),
                node: Some(node),
                listener: Some(l),
            },
            p,
        ))
    }
    /// Accept pgwire connections until the listener is dropped or fails to be
    /// adopted by Tokio. Returns immediately when the server is disabled.
    ///
    /// # Panics
    ///
    /// Panics if an enabled server lacks its config/node (an internal
    /// invariant), if the Tokio runtime cannot be built, or if the pre-bound
    /// listener cannot be adopted by Tokio.
    /// # Errors
    /// Returns an error when sessions do not finish within the drain deadline.
    pub fn serve_forever(&mut self) -> Result<(), String> {
        let Some(l) = self.listener.take() else {
            // sql language disabled: nothing to serve.
            return Ok(());
        };
        let cfg = self.cfg.clone().expect("enabled pgwire has config");
        let node = self.node.clone().expect("enabled pgwire has node");
        let h = Handlers::new(cfg, Arc::clone(&node));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let outcome = rt.block_on(async move {
            let l = tokio::net::TcpListener::from_std(l).unwrap();
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = l.accept() => {
                        let (s, _) = accepted.map_err(|error| format!("pgwire accept failed: {error}"))?;
                        // C7: bound live sessions; refuse (close) beyond the cap
                        // instead of accepting unbounded connections.
                        if connections.len() >= MAX_PGWIRE_CONNECTIONS {
                            tracing::warn!(limit = MAX_PGWIRE_CONNECTIONS, "pgwire connection limit reached; refusing");
                            drop(s);
                            continue;
                        }
                        connections.spawn(process_socket(s, None, h.clone()));
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                    () = tokio::time::sleep(std::time::Duration::from_millis(25)) => {
                        if node.store().maintenance_aborted() {
                            tracing::info!("pgwire transport draining after store shutdown");
                            break;
                        }
                    }
                }
            }
            let drain = async { while connections.join_next().await.is_some() {} };
            if tokio::time::timeout(std::time::Duration::from_secs(5), drain)
                .await
                .is_err()
            {
                tracing::warn!("pgwire transport drain deadline elapsed");
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                return Err("pgwire shutdown drain deadline elapsed".into());
            }
            Ok(())
        });
        rt.shutdown_timeout(std::time::Duration::from_secs(1));
        outcome
    }
}
/// True when `listen_addr` is a loopback address (127.0.0.1, `::1`, or
/// localhost); only loopback binds are safe for cleartext transports.
pub(crate) fn is_loopback(addr: &str) -> bool {
    matches!(addr, "127.0.0.1" | "::1" | "localhost") || addr.starts_with("127.")
}

/// Refuse to expose cleartext transports (pgwire/gRPC/Bolt) on a non-loopback
/// `--listen` address unless the operator explicitly acknowledges it via
/// `--allow-insecure-listen`. Loopback binds and HTTP-with-TLS are unaffected.
/// Checked on the effective configuration (after `config.yml` is applied),
/// counting only the transports that will actually bind.
fn guard_insecure_listen(cfg: &ServerConfig) -> Result<(), String> {
    let pgwire_enabled = cfg.pgwire_port != 0 && cfg.languages.sql;
    let cleartext_enabled = pgwire_enabled || cfg.grpc_port != 0 || crate::bolt::bolt_enabled(cfg);
    // HTTP has signed, tenant-bound capabilities. pgwire, gRPC, and Bolt
    // currently authenticate only the global boot token, so exposing any of
    // them directly on a multitenant non-loopback listener would let a holder
    // select an arbitrary tenant scope. They remain safe behind a local trusted
    // proxy, or for a single-tenant server. `--allow-insecure-listen`
    // acknowledges cleartext; it must not disable this tenancy guard.
    if cfg.multitenant && cleartext_enabled && !is_loopback(&cfg.listen_addr) {
        return Err(
            "refusing to bind pgwire/gRPC/Bolt on a non-loopback multitenant listener: these transports require tenant-bound credentials or a trusted local proxy".into(),
        );
    }
    if cleartext_enabled && !is_loopback(&cfg.listen_addr) && !cfg.allow_insecure_listen {
        return Err(format!(
            "refusing to bind pgwire/gRPC/Bolt (cleartext) to non-loopback listen address `{}` without --allow-insecure-listen; keep --listen loopback or add the explicit flag",
            cfg.listen_addr
        ));
    }
    Ok(())
}

type Gateway = (
    HttpRouter,
    u16,
    PgWireServer,
    u16,
    crate::GrpcServer,
    u16,
    crate::BoltServer,
    u16,
);

/// Maximum concurrent pgwire sessions (C7, cloud-capacity F8).
const MAX_PGWIRE_CONNECTIONS: usize = 1024;

/// Boot the node and bind every enabled transport (HTTP, pgwire, gRPC, Bolt).
///
/// # Errors
///
/// Returns a message when the listen security guard refuses the bind, the node
/// cannot bootstrap, or any enabled transport cannot bind its address.
pub fn new_gateway(cfg: ServerConfig, port: u16) -> Result<Gateway, String> {
    new_gateway_with_boot_listener(cfg, port, None)
}

/// [`new_gateway`] that adopts the socket a [`crate::boot::BootListener`]
/// served probes on during bootstrap.
///
/// # Errors
/// Returns a message when the node cannot bootstrap or a transport cannot bind.
pub fn new_gateway_with_boot_listener(
    mut cfg: ServerConfig,
    port: u16,
    boot_listener: Option<crate::boot::BootListener>,
) -> Result<Gateway, String> {
    // CLI/env pgwire port is the fallback; a stored config.yml may override it.
    cfg.pgwire_port = port;
    let node = Arc::new(Node::bootstrap(&mut cfg)?);
    guard_insecure_listen(&cfg)?;
    let (http, http_port) = match boot_listener {
        Some(listener) => {
            crate::http::bind_router_with_listener(cfg.clone(), node.clone(), listener.finish()?)?
        }
        None => crate::http::bind_router(cfg.clone(), node.clone())?,
    };
    let (pgwire, pgwire_port) = if cfg.languages.sql {
        PgWireServer::bind(cfg.clone(), node.clone(), cfg.pgwire_port)?
    } else {
        // `sql` language disabled: do not bind the PostgreSQL wire protocol.
        (PgWireServer::disabled(), 0)
    };
    let (grpc, grpc_port) = crate::grpc::bind_grpc(&cfg, node.clone())?;
    let (bolt, bolt_port) = crate::bolt::bind_bolt(&cfg, node)?;
    Ok((
        http,
        http_port,
        pgwire,
        pgwire_port,
        grpc,
        grpc_port,
        bolt,
        bolt_port,
    ))
}

#[derive(Clone)]
struct Handlers {
    auth: Arc<CleartextPasswordAuthStartupHandler<DbAuth, DefaultServerParameterProvider>>,
    db: Arc<DbHandler>,
}
impl Handlers {
    fn new(c: ServerConfig, n: Arc<Node>) -> Self {
        let mut p = DefaultServerParameterProvider::default();
        p.server_version = "16.0-rushdb".into();
        let multitenant = c.multitenant;
        Self {
            auth: Arc::new(CleartextPasswordAuthStartupHandler::new(
                DbAuth {
                    token: c.token,
                    multitenant,
                },
                p,
            )),
            db: Arc::new(DbHandler {
                node: n,
                multitenant,
                request_timeout_ms: c.request_timeout_ms,
            }),
        }
    }
}
impl PgWireServerHandlers for Handlers {
    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        self.auth.clone()
    }
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.db.clone()
    }
    #[allow(refining_impl_trait)]
    fn extended_query_handler(
        &self,
    ) -> Arc<impl ExtendedQueryHandler<Statement = PreparedSql, QueryParser = SqlQueryParser>> {
        self.db.clone()
    }
}
#[derive(Debug)]
struct DbAuth {
    token: String,
    multitenant: bool,
}
#[async_trait]
impl AuthSource for DbAuth {
    async fn get_password(&self, l: &LoginInfo) -> PgWireResult<Password> {
        if self.multitenant
            && l.database()
                .map(str::trim)
                .as_ref()
                .is_none_or(|database| database.is_empty())
        {
            return Err(err("3D000", "database is required as tenant scope"));
        }
        Ok(Password::new(None, self.token.as_bytes().to_vec()))
    }
}
struct DbHandler {
    node: Arc<Node>,
    multitenant: bool,
    request_timeout_ms: u64,
}

#[derive(Debug, Clone)]
struct PreparedSql {
    sql: String,
    parameter_types: Vec<Type>,
    result_fields: Vec<(String, Type)>,
}

/// A pgwire execution has either a row set or PostgreSQL's ordinary command
/// completion tag. Keeping this distinction prevents DML from pretending to
/// be a zero-column SELECT to clients and drivers.
enum PgExecution {
    Rows(Vec<Map<String, Value>>),
    Command(Tag),
}

struct SqlQueryParser {
    node: Arc<Node>,
    multitenant: bool,
}

impl std::fmt::Debug for SqlQueryParser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlQueryParser")
            .field("multitenant", &self.multitenant)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl QueryParser for SqlQueryParser {
    type Statement = PreparedSql;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        client_types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if is_transaction_control(sql) {
            return Err(err("0A000", TRANSACTIONS_UNSUPPORTED.to_owned()));
        }
        if let Some(command) = settings::parse(sql) {
            return Ok(PreparedSql {
                sql: sql.to_owned(),
                parameter_types: Vec::new(),
                result_fields: match command {
                    settings::Command::Show(setting) => {
                        vec![(setting.name().to_owned(), Type::TEXT)]
                    }
                    _ => Vec::new(),
                },
            });
        }
        if let Some(result_fields) = builtin_result_fields(sql) {
            return Ok(PreparedSql {
                sql: sql.to_owned(),
                parameter_types: Vec::new(),
                result_fields,
            });
        }
        if mutation_tag(sql).is_some_and(|tag| tag.ends_with("INDEX")) {
            // Index DDL binds no parameters and returns no rows.
            return Ok(PreparedSql {
                sql: sql.to_owned(),
                parameter_types: Vec::new(),
                result_fields: Vec::new(),
            });
        }
        if mutation_tag(sql).is_some() {
            // DML has a separate descriptor because the read descriptor
            // deliberately rejects mutations. This gives a standard client
            // concrete parameter OIDs before Bind without executing a write.
            let tenant = scope(client, self.multitenant);
            let lmpg = RequestScope::new(&self.node, tenant).lmpg();
            let description =
                rushdb_sql::describe_sql_mutation_async(&lmpg, sql, Some(self.node.text_indexes()))
                    .await;
            let description = description.map_err(|error| err("42601", error.to_string()))?;
            let inferred = description
                .parameters
                .iter()
                .map(|(_, data_type)| pg_type(data_type))
                .collect::<Vec<_>>();
            return Ok(PreparedSql {
                sql: sql.to_owned(),
                parameter_types: (0..inferred.len().max(client_types.len()))
                    .map(|index| {
                        client_types
                            .get(index)
                            .and_then(Clone::clone)
                            .unwrap_or_else(|| {
                                inferred.get(index).cloned().unwrap_or(Type::UNKNOWN)
                            })
                    })
                    .collect(),
                result_fields: Vec::new(),
            });
        }
        let tenant = scope(client, self.multitenant);
        let lmpg = RequestScope::new(&self.node, tenant).lmpg();
        let description =
            rushdb_sql::describe_sql_async(&lmpg, sql, Some(self.node.text_indexes())).await;
        let description = description.map_err(|error| err("42601", error.to_string()))?;

        let inferred = description
            .parameters
            .iter()
            .map(|(_, data_type)| pg_type(data_type))
            .collect::<Vec<_>>();
        let parameter_types = (0..inferred.len().max(client_types.len()))
            .map(|index| {
                client_types
                    .get(index)
                    .and_then(Clone::clone)
                    .unwrap_or_else(|| inferred.get(index).cloned().unwrap_or(Type::UNKNOWN))
            })
            .collect();
        let result_fields = description
            .columns
            .into_iter()
            .map(|column| (column.name, pg_type(&column.data_type)))
            .collect();
        Ok(PreparedSql {
            sql: sql.to_owned(),
            parameter_types,
            result_fields,
        })
    }

    fn get_parameter_types(&self, statement: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(statement.parameter_types.clone())
    }

    fn get_result_schema(
        &self,
        statement: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(prepared_fields(
            &statement.result_fields,
            column_format.unwrap_or(&Format::UnifiedText),
        ))
    }
}

fn pg_type(data_type: &rushdb_sql::SqlType) -> Type {
    use rushdb_sql::SqlType;
    match data_type {
        SqlType::Boolean => Type::BOOL,
        SqlType::Int16 | SqlType::UInt16 => Type::INT2,
        SqlType::Int32 | SqlType::UInt32 => Type::INT4,
        SqlType::Int64 | SqlType::UInt64 => Type::INT8,
        SqlType::Float32 => Type::FLOAT4,
        SqlType::Float64 => Type::FLOAT8,
        SqlType::Utf8 => Type::TEXT,
        SqlType::Binary => Type::BYTEA,
        SqlType::Date => Type::DATE,
        SqlType::Time => Type::TIME,
        SqlType::Timestamp => Type::TIMESTAMP,
        SqlType::Json => Type::JSONB,
        SqlType::Unknown => Type::UNKNOWN,
    }
}

fn prepared_fields(fields: &[(String, Type)], format: &Format) -> Vec<FieldInfo> {
    fields
        .iter()
        .enumerate()
        .map(|(index, (name, data_type))| {
            FieldInfo::new(
                name.clone(),
                None,
                None,
                data_type.clone(),
                format.format_for(index),
            )
        })
        .collect()
}
fn err(c: &str, m: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), c.into(), m.into())))
}

/// Answer a session-settings statement: `SHOW` returns its value,
/// `SET`/`RESET` their command tags.
fn setting_response<C: ClientInfo>(
    c: &mut C,
    command: settings::Command,
    format: &Format,
) -> PgWireResult<Response> {
    let tag = match command {
        settings::Command::Set(_, Some(_)) => "SET",
        _ => "RESET",
    };
    Ok(match settings::apply(c.metadata_mut(), command)? {
        Some(rows) => {
            let f = Arc::new(fields(&rows, format));
            Response::Query(QueryResponse::new(f.clone(), encode(rows, f)))
        }
        None => Response::Execution(Tag::new(tag)),
    })
}
fn scope<C: ClientInfo>(c: &C, tenant: bool) -> Option<String> {
    tenant
        .then(|| c.metadata().get("database").cloned())
        .flatten()
}
fn perr(e: ProtocolError) -> PgWireError {
    let c = sqlstate(e.code);
    err(c, e.message)
}

/// Shared pgwire SQLSTATE taxonomy: the same `ErrorCode` maps to the same
/// SQLSTATE on every adapter. Add below — do not reorder.
fn sqlstate(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::UnsupportedVersion | ErrorCode::LanguageDisabled | ErrorCode::Unsupported => {
            "0A000"
        }
        ErrorCode::ProtocolViolation => "08P01",
        ErrorCode::InvalidRequest => "42601",
        ErrorCode::Unauthorized => "42501",
        ErrorCode::MissingScope | ErrorCode::ScopeMismatch => "3D000",
        ErrorCode::NotFound => "42P01",
        ErrorCode::Conflict => "23505",
        ErrorCode::WriteConflict => "40001",
        ErrorCode::Cancelled => "57014",
        ErrorCode::ResourceExhausted => "53200",
        ErrorCode::Unavailable => "57P03",
        ErrorCode::ExecutionFailed => "XX000",
    }
}
fn builtin(sql: &str, tenant: Option<&str>, user: &str) -> Option<Vec<Map<String, Value>>> {
    let normalized = normalized_sql(sql);
    let entry = match normalized.as_str() {
        "select 1" => Some(("?column?", serde_json::json!(1))),
        "select version()" => Some(("version", serde_json::json!("RushDB pgwire 0.40"))),
        "show server_version" => Some(("server_version", serde_json::json!("16.0-rushdb"))),
        "select current_database()" => Some((
            "current_database",
            serde_json::json!(tenant.unwrap_or("rushdb")),
        )),
        "select current_user" | "select current_user()" => {
            Some(("current_user", serde_json::json!(user)))
        }
        _ => None,
    };
    entry.map(|(name, value)| {
        let mut row = Map::new();
        row.insert(name.into(), value);
        vec![row]
    })
}

fn normalized_sql(s: &str) -> String {
    s.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn builtin_result_fields(sql: &str) -> Option<Vec<(String, Type)>> {
    match normalized_sql(sql).as_str() {
        "select 1" => Some(vec![("?column?".into(), Type::INT4)]),
        "select version()" => Some(vec![("version".into(), Type::TEXT)]),
        "show server_version" => Some(vec![("server_version".into(), Type::TEXT)]),
        "select current_database()" => Some(vec![("current_database".into(), Type::TEXT)]),
        "select current_user" | "select current_user()" => {
            Some(vec![("current_user".into(), Type::TEXT)])
        }
        _ => None,
    }
}
async fn run_async(
    node: Arc<Node>,
    tenant: Option<String>,
    statement: String,
    parameters: Option<Value>,
    request_timeout_ms: u64,
    session: settings::SessionLimits,
) -> Result<PgExecution, ProtocolError> {
    tokio::task::spawn_blocking(move || {
        run(
            &node,
            tenant.as_deref(),
            &statement,
            "rushdb",
            parameters,
            request_timeout_ms,
            session,
        )
    })
    .await
    .map_err(|error| ProtocolError::new(ErrorCode::Unavailable, error.to_string()))?
}

fn run(
    node: &Arc<Node>,
    tenant: Option<&str>,
    sql: &str,
    user: &str,
    parameters: Option<Value>,
    request_timeout_ms: u64,
    session: settings::SessionLimits,
) -> Result<PgExecution, ProtocolError> {
    let statement = sql.trim().trim_end_matches(';');
    // The session's `statement_timeout` may only shorten the server's.
    let timeout_ms =
        crate::protocol::effective_deadline_ms(session.timeout_ms, request_timeout_ms).unwrap_or(0);
    if is_transaction_control(statement) {
        return Err(ProtocolError::new(
            ErrorCode::Unsupported,
            TRANSACTIONS_UNSUPPORTED,
        ));
    }
    if let Some(rows) = builtin(statement, tenant, user) {
        return Ok(PgExecution::Rows(rows));
    }
    let mutation = mutation_tag(statement);
    let envelope = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: None,
        context: RequestContext {
            scope: tenant.map(str::to_string),
            deadline_ms: (timeout_ms > 0).then_some(timeout_ms),
            query_memory_max_bytes: session.query_memory_max_bytes,
            unindexed_walk_max_owners: session.unindexed_walk_max_owners,
        },
        operation: if mutation.is_some() {
            Operation::Mutate {
                mutation: rushdb_protocol::Mutation::Statement {
                    language: rushdb_protocol::StatementLanguage::Sql,
                    statement: statement.trim().trim_end_matches(';').into(),
                    parameters,
                },
            }
        } else {
            Operation::Query {
                query: Query::Statement {
                    language: rushdb_protocol::StatementLanguage::Sql,
                    statement: statement.trim().trim_end_matches(';').into(),
                    parameters,
                },
            }
        },
    };
    let started = std::time::Instant::now();
    // Deadline-derived cancellation (same as the HTTP path): a long-running
    // pgwire query aborts with a structured Cancelled error (SQLSTATE 57014).
    let is_cancelled = move || {
        timeout_ms > 0
            && u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX) >= timeout_ms
    };
    let scope_for_job = tenant.map(str::to_string);
    let tenant_id = node.schedule_key(tenant);
    let node_for_job = Arc::clone(node);
    let result = node
        .schedule_for_tenant(tenant_id, move || {
            let result = crate::protocol::ProtocolExecutor::new(&node_for_job, scope_for_job)
                .execute_cancellable(envelope, &is_cancelled);
            result
        })

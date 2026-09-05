//! gRPC front end for the SQL engine.
//!
//! The engine itself is synchronous and single-process. This module puts a
//! network boundary in front of it: a tonic service that owns a [`KVEngine`],
//! hands out session handles, and runs each statement on the blocking thread
//! pool so the async reactor is never blocked by a scan.
//!
//! Result delivery comes in two shapes. `Execute` returns the whole result in
//! one message. `ExecuteStream` sends column metadata first and then rows in
//! batches, so a large scan is not carried in a single frame.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::error::Error;
use crate::sql::engine::kv::KVEngine;
use crate::sql::engine::Engine as SqlEngine;
use crate::sql::executor::ResultSet;
use crate::sql::types::Value as SqlValue;
use crate::storage::engine::Engine as StorageEngine;

pub mod proto {
    tonic::include_proto!("sharkdb.v1");
}

use proto::sql_server::{Sql, SqlServer};
use proto::{
    CloseSessionRequest, CloseSessionResponse, CreateSessionRequest, CreateSessionResponse,
    CreateTableResult, ExecuteRequest, ExecuteResponse, ExecuteStreamRequest,
    ExecuteStreamResponse, ExplainResult, HealthRequest, HealthResponse, MutationResult, Null,
    Row as ProtoRow, RowBatch, ScanMetadata, ScanResult, Value as ProtoValue,
};

/// Rows per batch when the client does not ask for a specific size.
pub const DEFAULT_BATCH_SIZE: usize = 512;

/// Upper bound on batch size, so a client cannot ask the server to build one
/// enormous message on its behalf.
pub const MAX_BATCH_SIZE: usize = 8192;

// ---------------------------------------------------------------------------
// Type conversions
// ---------------------------------------------------------------------------

impl From<SqlValue> for ProtoValue {
    fn from(value: SqlValue) -> Self {
        use proto::value::Kind;
        let kind = match value {
            SqlValue::Null => Kind::Null(Null {}),
            SqlValue::Boolean(b) => Kind::Boolean(b),
            SqlValue::Integer(i) => Kind::Integer(i),
            SqlValue::Float(f) => Kind::DoubleValue(f),
            SqlValue::String(s) => Kind::StringValue(s),
        };
        ProtoValue { kind: Some(kind) }
    }
}

impl From<ProtoValue> for SqlValue {
    fn from(value: ProtoValue) -> Self {
        use proto::value::Kind;
        match value.kind {
            Some(Kind::Null(_)) | None => SqlValue::Null,
            Some(Kind::Boolean(b)) => SqlValue::Boolean(b),
            Some(Kind::Integer(i)) => SqlValue::Integer(i),
            Some(Kind::DoubleValue(f)) => SqlValue::Float(f),
            Some(Kind::StringValue(s)) => SqlValue::String(s),
        }
    }
}

fn encode_row(row: Vec<SqlValue>) -> ProtoRow {
    ProtoRow {
        values: row.into_iter().map(ProtoValue::from).collect(),
    }
}

/// Map an engine error onto the gRPC status code that describes it.
///
/// A parse error is the caller's fault, a write conflict is retryable, and
/// anything else is ours.
fn map_error(err: Error) -> Status {
    match err {
        Error::Parse(msg) => Status::invalid_argument(msg),
        Error::WriteConflict => Status::aborted("write conflict, retry the transaction"),
        Error::Internal(msg) => Status::internal(msg),
    }
}

impl From<ResultSet> for ExecuteResponse {
    fn from(result: ResultSet) -> Self {
        use proto::execute_response::Result as R;
        let result = match result {
            ResultSet::CreateTable { table_name } => R::CreateTable(CreateTableResult {
                table_name,
            }),
            ResultSet::Insert { count } => R::Insert(MutationResult {
                count: count as u64,
            }),
            ResultSet::Update { count } => R::Update(MutationResult {
                count: count as u64,
            }),
            ResultSet::Delete { count } => R::Delete(MutationResult {
                count: count as u64,
            }),
            ResultSet::Scan { columns, rows } => R::Scan(ScanResult {
                columns,
                rows: rows.into_iter().map(encode_row).collect(),
            }),
            ResultSet::Explain { plan } => R::Explain(ExplainResult { plan }),
        };
        ExecuteResponse {
            result: Some(result),
        }
    }
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SessionEntry {
    /// Statements executed on this session. Cheap, and it makes the session
    /// registry observable rather than an opaque set of strings.
    statements: u64,
}

/// The gRPC service. Cloning is cheap: the engine is reference-counted and the
/// session registry is shared.
pub struct SqlService<E: StorageEngine> {
    engine: KVEngine<E>,
    sessions: Arc<Mutex<HashMap<String, SessionEntry>>>,
    next_session: Arc<AtomicU64>,
}

impl<E: StorageEngine> Clone for SqlService<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            sessions: self.sessions.clone(),
            next_session: self.next_session.clone(),
        }
    }
}

impl<E: StorageEngine> SqlService<E> {
    pub fn new(engine: KVEngine<E>) -> Self {
        Self {
            engine,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_session: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Number of sessions currently open.
    pub fn open_sessions(&self) -> usize {
        self.sessions.lock().map(|s| s.len()).unwrap_or(0)
    }

    /// Confirm the session exists and count the statement against it.
    ///
    /// An unknown session is rejected rather than silently auto-created, so a
    /// client that lost its handle finds out immediately.
    fn charge_session(&self, session_id: &str) -> Result<(), Status> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| Status::internal("session registry poisoned"))?;
        match sessions.get_mut(session_id) {
            Some(entry) => {
                entry.statements += 1;
                Ok(())
            }
            None => Err(Status::not_found(format!(
                "unknown session {session_id}, call CreateSession first"
            ))),
        }
    }
}

impl<E> SqlService<E>
where
    E: StorageEngine + Send + 'static,
{
    /// Run one statement on the blocking pool.
    ///
    /// The engine takes a mutex around the storage engine, so a long scan would
    /// otherwise stall every task sharing the reactor thread.
    async fn run(&self, sql: String) -> Result<ResultSet, Status> {
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || {
            let mut session = engine.session()?;
            session.execute(&sql)
        })
        .await
        .map_err(|err| Status::internal(format!("execution task failed: {err}")))?
        .map_err(map_error)
    }
}

type ExecuteStreamStream =
    Pin<Box<dyn Stream<Item = Result<ExecuteStreamResponse, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<E> Sql for SqlService<E>
where
    E: StorageEngine + Send + 'static,
{
    async fn create_session(
        &self,
        _request: Request<CreateSessionRequest>,
    ) -> Result<Response<CreateSessionResponse>, Status> {
        let id = self.next_session.fetch_add(1, Ordering::Relaxed);
        let session_id = format!("s-{id}");
        self.sessions
            .lock()
            .map_err(|_| Status::internal("session registry poisoned"))?
            .insert(session_id.clone(), SessionEntry { statements: 0 });
        Ok(Response::new(CreateSessionResponse { session_id }))
    }

    async fn close_session(
        &self,
        request: Request<CloseSessionRequest>,
    ) -> Result<Response<CloseSessionResponse>, Status> {
        let session_id = request.into_inner().session_id;
        let existed = self
            .sessions
            .lock()
            .map_err(|_| Status::internal("session registry poisoned"))?
            .remove(&session_id)
            .is_some();
        Ok(Response::new(CloseSessionResponse { existed }))
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let ExecuteRequest { session_id, sql } = request.into_inner();
        self.charge_session(&session_id)?;
        let result = self.run(sql).await?;
        Ok(Response::new(result.into()))
    }

    type ExecuteStreamStream = ExecuteStreamStream;

    async fn execute_stream(
        &self,
        request: Request<ExecuteStreamRequest>,
    ) -> Result<Response<Self::ExecuteStreamStream>, Status> {
        let ExecuteStreamRequest {
            session_id,
            sql,
            batch_size,
        } = request.into_inner();
        self.charge_session(&session_id)?;

        let batch_size = match batch_size as usize {
            0 => DEFAULT_BATCH_SIZE,
            n => n.min(MAX_BATCH_SIZE),
        };

        let result = self.run(sql).await?;

        use proto::execute_stream_response::Payload;
        let mut messages: Vec<Result<ExecuteStreamResponse, Status>> = Vec::new();

        match result {
            // Row-producing statements: metadata first, then batches.
            ResultSet::Scan { columns, rows } => {
                messages.push(Ok(ExecuteStreamResponse {
                    payload: Some(Payload::Metadata(ScanMetadata { columns })),
                }));
                for chunk in rows.chunks(batch_size) {
                    let batch = RowBatch {
                        rows: chunk.iter().cloned().map(encode_row).collect(),
                    };
                    messages.push(Ok(ExecuteStreamResponse {
                        payload: Some(Payload::Batch(batch)),
                    }));
                }
            }
            // Everything else produces a single summary message, so a client
            // can drive every statement through one code path.
            other => messages.push(Ok(ExecuteStreamResponse {
                payload: Some(Payload::Summary(other.into())),
            })),
        }

        Ok(Response::new(Box::pin(tokio_stream::iter(messages))))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            open_sessions: self.open_sessions() as u64,
        }))
    }
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

/// Wrap an engine in a ready-to-serve gRPC service.
pub fn service<E>(engine: KVEngine<E>) -> SqlServer<SqlService<E>>
where
    E: StorageEngine + Send + 'static,
{
    SqlServer::new(SqlService::new(engine))
}

/// Serve on `addr` until the process ends.
pub async fn serve<E>(
    addr: SocketAddr,
    engine: KVEngine<E>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: StorageEngine + Send + 'static,
{
    tonic::transport::Server::builder()
        .add_service(service(engine))
        .serve(addr)
        .await?;
    Ok(())
}

/// Serve on an already-bound listener.
///
/// Tests bind port 0 to get an ephemeral port, so they never collide when run
/// in parallel.
pub async fn serve_with_listener<E>(
    listener: tokio::net::TcpListener,
    engine: KVEngine<E>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: StorageEngine + Send + 'static,
{
    tonic::transport::Server::builder()
        .add_service(service(engine))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

//! Integration tests for the gRPC layer.
//!
//! Every test binds port 0, so the OS picks a free port and tests can run in
//! parallel without colliding. Traffic goes over a real TCP socket and a real
//! HTTP/2 connection; nothing here is mocked.

#![cfg(feature = "grpc")]

use std::time::Duration;

use sharkdb::grpc;
use sharkdb::grpc::proto::{
    execute_response, execute_stream_response, sql_client::SqlClient, value::Kind,
    CloseSessionRequest, CreateSessionRequest, ExecuteRequest, ExecuteStreamRequest, HealthRequest,
};
use sharkdb::sql::engine::kv::KVEngine;
use sharkdb::storage::memory::MemoryEngine;
use tokio_stream::StreamExt;

/// Start a server on an ephemeral port and return a connected client.
async fn start_server() -> SqlClient<tonic::transport::Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let engine = KVEngine::new(MemoryEngine::new());
    tokio::spawn(async move {
        grpc::serve_with_listener(listener, engine).await.unwrap();
    });

    // Retry the connect rather than sleeping a fixed amount: the server task
    // may not have reached accept() yet.
    let endpoint = format!("http://{addr}");
    for attempt in 0..50 {
        match SqlClient::connect(endpoint.clone()).await {
            Ok(client) => return client,
            Err(_) if attempt < 49 => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(err) => panic!("could not connect to {endpoint}: {err}"),
        }
    }
    unreachable!()
}

async fn open_session(client: &mut SqlClient<tonic::transport::Channel>) -> String {
    client
        .create_session(CreateSessionRequest {})
        .await
        .unwrap()
        .into_inner()
        .session_id
}

async fn run(
    client: &mut SqlClient<tonic::transport::Channel>,
    session_id: &str,
    sql: &str,
) -> execute_response::Result {
    client
        .execute(ExecuteRequest {
            session_id: session_id.to_string(),
            sql: sql.to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("response carried no result")
}

#[tokio::test]
async fn health_reports_version_and_session_count() {
    let mut client = start_server().await;

    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(health.open_sessions, 0);

    let a = open_session(&mut client).await;
    let b = open_session(&mut client).await;
    assert_ne!(a, b, "session ids must be distinct");

    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.open_sessions, 2);

    let closed = client
        .close_session(CloseSessionRequest { session_id: a })
        .await
        .unwrap()
        .into_inner();
    assert!(closed.existed);

    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.open_sessions, 1);
}

#[tokio::test]
async fn close_session_is_idempotent() {
    let mut client = start_server().await;
    let session_id = open_session(&mut client).await;

    let first = client
        .close_session(CloseSessionRequest {
            session_id: session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(first.existed);

    let second = client
        .close_session(CloseSessionRequest { session_id })
        .await
        .unwrap()
        .into_inner();
    assert!(!second.existed, "second close must report it was already gone");
}

#[tokio::test]
async fn ddl_dml_and_query_round_trip() {
    let mut client = start_server().await;
    let session = open_session(&mut client).await;

    match run(&mut client, &session, "create table t (id int primary key, name text);").await {
        execute_response::Result::CreateTable(r) => assert_eq!(r.table_name, "t"),
        other => panic!("expected CreateTable, got {other:?}"),
    }

    match run(&mut client, &session, "insert into t values (1, 'alpha');").await {
        execute_response::Result::Insert(r) => assert_eq!(r.count, 1),
        other => panic!("expected Insert, got {other:?}"),
    }
    run(&mut client, &session, "insert into t values (2, 'beta');").await;
    run(&mut client, &session, "insert into t values (3, 'gamma');").await;

    match run(&mut client, &session, "select * from t order by id;").await {
        execute_response::Result::Scan(scan) => {
            assert_eq!(scan.columns, vec!["id", "name"]);
            assert_eq!(scan.rows.len(), 3);
            let first = &scan.rows[0].values;
            assert!(matches!(first[0].kind, Some(Kind::Integer(1))));
            assert!(
                matches!(&first[1].kind, Some(Kind::StringValue(s)) if s == "alpha"),
                "unexpected first row: {first:?}"
            );
        }
        other => panic!("expected Scan, got {other:?}"),
    }

    match run(&mut client, &session, "update t set name = 'delta' where id = 2;").await {
        execute_response::Result::Update(r) => assert_eq!(r.count, 1),
        other => panic!("expected Update, got {other:?}"),
    }

    match run(&mut client, &session, "delete from t where id = 3;").await {
        execute_response::Result::Delete(r) => assert_eq!(r.count, 1),
        other => panic!("expected Delete, got {other:?}"),
    }

    match run(&mut client, &session, "select * from t;").await {
        execute_response::Result::Scan(scan) => assert_eq!(scan.rows.len(), 2),
        other => panic!("expected Scan, got {other:?}"),
    }
}

#[tokio::test]
async fn null_values_survive_the_wire() {
    let mut client = start_server().await;
    let session = open_session(&mut client).await;

    run(
        &mut client,
        &session,
        "create table n (id int primary key, note text);",
    )
    .await;
    run(&mut client, &session, "insert into n values (1, null);").await;

    match run(&mut client, &session, "select * from n;").await {
        execute_response::Result::Scan(scan) => {
            let note = &scan.rows[0].values[1];
            assert!(
                matches!(note.kind, Some(Kind::Null(_))),
                "NULL did not round-trip: {note:?}"
            );
        }
        other => panic!("expected Scan, got {other:?}"),
    }
}

#[tokio::test]
async fn explain_returns_the_optimized_plan() {
    let mut client = start_server().await;
    let session = open_session(&mut client).await;

    run(
        &mut client,
        &session,
        "create table e (id int primary key, score int);",
    )
    .await;

    match run(&mut client, &session, "explain select * from e where id = 3;").await {
        execute_response::Result::Explain(r) => {
            assert!(!r.plan.is_empty(), "EXPLAIN returned an empty plan");
        }
        other => panic!("expected Explain, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_delivers_metadata_then_batches() {
    let mut client = start_server().await;
    let session = open_session(&mut client).await;

    run(
        &mut client,
        &session,
        "create table s (id int primary key, v int);",
    )
    .await;
    for i in 1..=10 {
        run(
            &mut client,
            &session,
            &format!("insert into s values ({i}, {});", i * 10),
        )
        .await;
    }

    let mut stream = client
        .execute_stream(ExecuteStreamRequest {
            session_id: session.clone(),
            sql: "select * from s order by id;".to_string(),
            batch_size: 3,
        })
        .await
        .unwrap()
        .into_inner();

    let mut columns = Vec::new();
    let mut batch_sizes = Vec::new();
    let mut rows = 0;
    let mut saw_metadata_first = None;

    while let Some(message) = stream.next().await {
        match message.unwrap().payload.unwrap() {
            execute_stream_response::Payload::Metadata(meta) => {
                saw_metadata_first.get_or_insert(true);
                columns = meta.columns;
            }
            execute_stream_response::Payload::Batch(batch) => {
                saw_metadata_first.get_or_insert(false);
                batch_sizes.push(batch.rows.len());
                rows += batch.rows.len();
            }
            execute_stream_response::Payload::Summary(s) => panic!("unexpected summary {s:?}"),
        }
    }

    assert_eq!(saw_metadata_first, Some(true), "metadata must arrive first");
    assert_eq!(columns, vec!["id", "v"]);
    assert_eq!(rows, 10);
    assert_eq!(
        batch_sizes,
        vec![3, 3, 3, 1],
        "10 rows at batch_size 3 should arrive as 3+3+3+1"
    );
}

#[tokio::test]
async fn stream_sends_a_summary_for_non_row_statements() {
    let mut client = start_server().await;
    let session = open_session(&mut client).await;

    let mut stream = client
        .execute_stream(ExecuteStreamRequest {
            session_id: session.clone(),
            sql: "create table only (id int primary key);".to_string(),
            batch_size: 0,
        })
        .await
        .unwrap()
        .into_inner();

    let payload = stream.next().await.unwrap().unwrap().payload.unwrap();
    match payload {
        execute_stream_response::Payload::Summary(summary) => match summary.result.unwrap() {
            execute_response::Result::CreateTable(r) => assert_eq!(r.table_name, "only"),
            other => panic!("expected CreateTable summary, got {other:?}"),
        },
        other => panic!("expected a summary, got {other:?}"),
    }
    assert!(stream.next().await.is_none(), "stream should end after the summary");
}

#[tokio::test]
async fn unknown_session_is_rejected() {
    let mut client = start_server().await;

    let status = client
        .execute(ExecuteRequest {
            session_id: "s-does-not-exist".to_string(),
            sql: "select 1;".to_string(),
        })
        .await
        .expect_err("an unknown session must be rejected");
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn closed_session_stops_accepting_statements() {
    let mut client = start_server().await;
    let session = open_session(&mut client).await;

    run(&mut client, &session, "create table c (id int primary key);").await;
    client
        .close_session(CloseSessionRequest {
            session_id: session.clone(),
        })
        .await
        .unwrap();

    let status = client
        .execute(ExecuteRequest {
            session_id: session,
            sql: "select * from c;".to_string(),
        })
        .await
        .expect_err("a closed session must be rejected");
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn parse_errors_map_to_invalid_argument() {
    let mut client = start_server().await;
    let session = open_session(&mut client).await;

    let status = client
        .execute(ExecuteRequest {
            session_id: session,
            sql: "select from where;".to_string(),
        })
        .await
        .expect_err("a malformed statement must be rejected");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "parse failures are the caller's fault, not an internal error"
    );
}

#[tokio::test]
async fn missing_table_maps_to_internal_not_a_panic() {
    let mut client = start_server().await;
    let session = open_session(&mut client).await;

    let status = client
        .execute(ExecuteRequest {
            session_id: session.clone(),
            sql: "select * from nonexistent;".to_string(),
        })
        .await
        .expect_err("querying a missing table must return an error");
    assert_eq!(status.code(), tonic::Code::Internal);

    // The server must still be alive after an error.
    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.open_sessions, 1);
}

#[tokio::test]
async fn concurrent_sessions_share_one_engine() {
    let mut client = start_server().await;
    let setup = open_session(&mut client).await;
    run(
        &mut client,
        &setup,
        "create table shared (id int primary key, who text);",
    )
    .await;

    // Ten clients on their own sessions, writing at the same time.
    let mut handles = Vec::new();
    for i in 0..10 {
        let mut client = client.clone();
        handles.push(tokio::spawn(async move {
            let session = client
                .create_session(CreateSessionRequest {})
                .await
                .unwrap()
                .into_inner()
                .session_id;
            client
                .execute(ExecuteRequest {
                    session_id: session.clone(),
                    sql: format!("insert into shared values ({i}, 'w{i}');"),
                })
                .await
                .unwrap();
            client
                .close_session(CloseSessionRequest { session_id: session })
                .await
                .unwrap();
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    match run(&mut client, &setup, "select * from shared;").await {
        execute_response::Result::Scan(scan) => {
            assert_eq!(scan.rows.len(), 10, "every concurrent write should land");
        }
        other => panic!("expected Scan, got {other:?}"),
    }
}

#[tokio::test]
async fn batch_size_is_capped() {
    let mut client = start_server().await;
    let session = open_session(&mut client).await;

    run(
        &mut client,
        &session,
        "create table b (id int primary key);",
    )
    .await;
    run(&mut client, &session, "insert into b values (1);").await;

    // An absurd batch size must be clamped rather than honoured.
    let mut stream = client
        .execute_stream(ExecuteStreamRequest {
            session_id: session,
            sql: "select * from b;".to_string(),
            batch_size: u32::MAX,
        })
        .await
        .unwrap()
        .into_inner();

    let mut rows = 0;
    while let Some(message) = stream.next().await {
        if let Some(execute_stream_response::Payload::Batch(batch)) = message.unwrap().payload {
            rows += batch.rows.len();
        }
    }
    assert_eq!(rows, 1);
}

//! Minimal gRPC client: opens a session, runs a few statements, streams a scan.
//!
//! Start the server first:
//!   cargo run --features grpc --bin sharkdb-server -- --engine memory
//! Then:
//!   cargo run --features grpc --example grpc_client

use sharkdb::grpc::proto::{
    execute_response, execute_stream_response, sql_client::SqlClient, value::Kind,
    CloseSessionRequest, CreateSessionRequest, ExecuteRequest, ExecuteStreamRequest, HealthRequest,
    Value,
};
use tokio_stream::StreamExt;

fn render(value: &Value) -> String {
    match &value.kind {
        Some(Kind::Null(_)) | None => "NULL".to_string(),
        Some(Kind::Boolean(b)) => b.to_string(),
        Some(Kind::Integer(i)) => i.to_string(),
        Some(Kind::DoubleValue(f)) => f.to_string(),
        Some(Kind::StringValue(s)) => s.clone(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::args().nth(1).unwrap_or_else(|| "http://127.0.0.1:50051".to_string());
    let mut client = SqlClient::connect(endpoint).await?;

    let health = client.health(HealthRequest {}).await?.into_inner();
    println!("connected to SharkDB {}", health.version);

    let session_id = client
        .create_session(CreateSessionRequest {})
        .await?
        .into_inner()
        .session_id;
    println!("session {session_id}");

    let statements = [
        "create table movie (id int primary key, title text, score float);",
        "insert into movie values (1, 'The Shark', 8.4);",
        "insert into movie values (2, 'Deep Water', 7.1);",
        "insert into movie values (3, 'Open Sea', 9.2);",
    ];
    for sql in statements {
        let response = client
            .execute(ExecuteRequest {
                session_id: session_id.clone(),
                sql: sql.to_string(),
            })
            .await?
            .into_inner();
        match response.result {
            Some(execute_response::Result::CreateTable(r)) => {
                println!("created table {}", r.table_name)
            }
            Some(execute_response::Result::Insert(r)) => println!("inserted {} row(s)", r.count),
            other => println!("{other:?}"),
        }
    }

    // Stream a scan back two rows at a time, to show the batching.
    println!("\nstreaming: select * from movie where score > 7.5 order by score desc;");
    let mut stream = client
        .execute_stream(ExecuteStreamRequest {
            session_id: session_id.clone(),
            sql: "select * from movie where score > 7.5 order by score desc;".to_string(),
            batch_size: 2,
        })
        .await?
        .into_inner();

    let mut batches = 0;
    while let Some(message) = stream.next().await {
        match message?.payload {
            Some(execute_stream_response::Payload::Metadata(meta)) => {
                println!("  columns: {}", meta.columns.join(", "))
            }
            Some(execute_stream_response::Payload::Batch(batch)) => {
                batches += 1;
                for row in batch.rows {
                    let cells: Vec<String> = row.values.iter().map(render).collect();
                    println!("  {}", cells.join(" | "));
                }
            }
            Some(execute_stream_response::Payload::Summary(summary)) => {
                println!("  summary: {summary:?}")
            }
            None => {}
        }
    }
    println!("  delivered in {batches} batch(es)");

    client
        .close_session(CloseSessionRequest { session_id })
        .await?;
    Ok(())
}

//! SharkDB gRPC server.
//!
//! Usage:
//!   sharkdb-server [--addr 127.0.0.1:50051] [--data <dir>] [--engine lsm|bitcask|memory]

use std::net::SocketAddr;
use std::path::PathBuf;

use sharkdb::grpc;
use sharkdb::sql::engine::kv::KVEngine;
use sharkdb::storage::{disk::DiskEngine, lsm::LsmEngine, memory::MemoryEngine};

#[derive(Debug)]
struct Args {
    addr: SocketAddr,
    data: PathBuf,
    engine: String,
}

fn parse_args() -> Result<Args, String> {
    let mut addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
    let mut data = PathBuf::from("sharkdb-data");
    let mut engine = "lsm".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--addr" => {
                let raw = args.next().ok_or("--addr needs a value")?;
                addr = raw.parse().map_err(|e| format!("bad --addr {raw}: {e}"))?;
            }
            "--data" => data = PathBuf::from(args.next().ok_or("--data needs a value")?),
            "--engine" => engine = args.next().ok_or("--engine needs a value")?,
            "-h" | "--help" => {
                println!(
                    "sharkdb-server [--addr 127.0.0.1:50051] [--data <dir>] [--engine lsm|bitcask|memory]"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Args { addr, data, engine })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = match parse_args() {
        Ok(args) => args,
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(2);
        }
    };

    println!(
        "SharkDB gRPC server on {} (engine: {}, data: {})",
        args.addr,
        args.engine,
        args.data.display()
    );

    // Each arm builds a differently-typed engine, so serving happens inside the
    // match rather than after it.
    match args.engine.as_str() {
        "memory" => grpc::serve(args.addr, KVEngine::new(MemoryEngine::new())).await,
        "bitcask" => {
            std::fs::create_dir_all(&args.data)?;
            let engine = DiskEngine::new(args.data.join("sharkdb.log"))?;
            grpc::serve(args.addr, KVEngine::new(engine)).await
        }
        "lsm" => {
            std::fs::create_dir_all(&args.data)?;
            let engine = LsmEngine::new(args.data.clone())?;
            grpc::serve(args.addr, KVEngine::new(engine)).await
        }
        other => {
            eprintln!("unknown engine {other}, expected lsm, bitcask or memory");
            std::process::exit(2);
        }
    }
}

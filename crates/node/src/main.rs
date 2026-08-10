mod server;
mod service;

use clap::Parser;
use proto::kv::kv_service_server::KvServiceServer;
use proto::raft::raft_service_server::RaftServiceServer;
use proto::txn::txn_service_server::TxnServiceServer;
use serde::Deserialize;
use server::{NodeState, ServerConfig};
use service::{KvServiceImpl, RaftServiceImpl, TxnServiceImpl};
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use tonic::transport::Server;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about = "Distributed ACID Key-Value Server Node")]
struct Args {
    #[arg(short, long, default_value_t = 1)]
    node_id: u64,

    #[arg(short, long, default_value = "127.0.0.1:50051")]
    addr: SocketAddr,

    #[arg(short, long, default_value = "./data/node-1")]
    data_dir: PathBuf,

    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    node_id: Option<u64>,
    grpc_addr: Option<String>,
    storage_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let mut node_id = args.node_id;
    let mut addr = args.addr;
    let mut data_dir = args.data_dir;

    if let Some(config_path) = args.config {
        let content = fs::read_to_string(&config_path)?;
        let parsed: ConfigFile = toml::from_str(&content)?;
        if let Some(id) = parsed.node_id {
            node_id = id;
        }
        if let Some(g_addr) = parsed.grpc_addr {
            addr = g_addr.parse()?;
        }
        if let Some(path) = parsed.storage_path {
            data_dir = path;
        }
    }

    let config = ServerConfig {
        node_id,
        addr,
        data_dir,
        ..Default::default()
    };

    info!(node_id = config.node_id, addr = %config.addr, "Initializing Node...");

    let node_state = NodeState::new(config.clone())?;

    let kv_service = KvServiceImpl::new(node_state.clone());
    let txn_service = TxnServiceImpl::new(node_state.clone());
    let raft_service = RaftServiceImpl::new(node_state.clone());

    info!("Starting gRPC server listening on {}", config.addr);

    Server::builder()
        .add_service(KvServiceServer::new(kv_service))
        .add_service(TxnServiceServer::new(txn_service))
        .add_service(RaftServiceServer::new(raft_service))
        .serve(config.addr)
        .await?;

    Ok(())
}

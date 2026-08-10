use raft_engine::{MultiRaftNode, RaftConfig, Region, RegionRouter};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use storage::{StorageConfig, StorageEngine};
use txn_coordinator::{HybridLogicalClock, TransactionCoordinator};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub node_id: u64,
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
    pub raft_tick_ms: u64,
    pub storage_config: StorageConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            addr: "127.0.0.1:50051".parse().unwrap(),
            data_dir: PathBuf::from("./data/node-1"),
            raft_tick_ms: 100,
            storage_config: StorageConfig::default(),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct NodeState {
    pub config: ServerConfig,
    pub storage: StorageEngine,
    pub hlc: HybridLogicalClock,
    pub coordinator: Arc<TransactionCoordinator>,
    pub router: RegionRouter,
}

impl NodeState {
    pub fn new(config: ServerConfig) -> Result<Self, storage::StorageError> {
        let storage = StorageEngine::open_with_config(&config.data_dir, config.storage_config.clone())?;
        let hlc = HybridLogicalClock::new();
        let coordinator = Arc::new(TransactionCoordinator::new(storage.clone(), hlc.clone()));
        let router = RegionRouter::new();

        router.insert_region(Region {
            id: 1,
            start_key: vec![],
            end_key: vec![],
            peers: vec![config.node_id],
        });

        let raft_cfg = RaftConfig {
            node_id: config.node_id,
            ..Default::default()
        };
        let _raft_node = MultiRaftNode::new(1, vec![config.node_id], &raft_cfg, storage.clone())
            .expect("Failed to initialize Raft node");

        Ok(Self {
            config,
            storage,
            hlc,
            coordinator,
            router,
        })
    }
}

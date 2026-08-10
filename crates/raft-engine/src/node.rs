use crate::config::RaftConfig;
use crate::store::RaftStorage;
use raft::eraftpb::Message;
use raft::{Config as RawRaftConfig, RawNode};
use slog::{o, Discard, Logger};
use storage::StorageEngine;
use thiserror::Error;
use tracing::info;

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Raft error: {0}")]
    Raft(#[from] raft::Error),
}

pub struct MultiRaftNode {
    pub region_id: u64,
    pub raw_node: RawNode<RaftStorage>,
    pub storage: RaftStorage,
}

impl MultiRaftNode {
    pub fn new(
        region_id: u64,
        _peers: Vec<u64>,
        cfg: &RaftConfig,
        engine: StorageEngine,
    ) -> Result<Self, NodeError> {
        let storage = RaftStorage::new(engine, region_id);

        let raft_cfg = RawRaftConfig {
            id: cfg.node_id,
            election_tick: cfg.election_tick,
            heartbeat_tick: cfg.heartbeat_tick,
            max_size_per_msg: cfg.max_size_per_msg,
            max_inflight_msgs: cfg.max_inflight_msgs,
            applied: 0,
            ..Default::default()
        };

        let logger = Logger::root(Discard, o!());
        let raw_node = RawNode::new(&raft_cfg, storage.clone(), &logger)?;

        info!(
            region_id = region_id,
            node_id = cfg.node_id,
            "Initialized MultiRaftNode"
        );

        Ok(Self {
            region_id,
            raw_node,
            storage,
        })
    }

    pub fn tick(&mut self) {
        self.raw_node.tick();
    }

    pub fn step(&mut self, msg: Message) -> Result<(), NodeError> {
        self.raw_node.step(msg)?;
        Ok(())
    }

    pub fn propose(&mut self, data: Vec<u8>) -> Result<(), NodeError> {
        self.raw_node.propose(vec![], data)?;
        Ok(())
    }
}

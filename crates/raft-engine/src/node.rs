use crate::config::RaftConfig;
use crate::store::RaftStorage;
use raft::eraftpb::Message;
use raft::{Config as RawRaftConfig, RawNode};
use serde::{Deserialize, Serialize};
use slog::{o, Discard, Logger};
use storage::StorageEngine;
use thiserror::Error;
use tracing::info;

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Raft error: {0}")]
    Raft(#[from] raft::Error),
    #[error("Raft storage error: {0}")]
    RaftStorage(#[from] raft::StorageError),
    #[error("Storage error: {0}")]
    Storage(#[from] storage::StorageError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCmd {
    SplitCmd {
        region_id: u64,
        split_key: Vec<u8>,
        new_region_id: u64,
    },
    TxnCmd {
        payload: Vec<u8>,
    },
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

    pub fn propose_cmd(&mut self, cmd: &RaftCmd) -> Result<(), NodeError> {
        let data = serde_json::to_vec(cmd).unwrap_or_default();
        self.propose(data)
    }

    /// Process Raft ready state: persists hard state/log entries, collects outgoing messages,
    /// and returns committed payloads.
    pub fn process_ready(&mut self) -> Result<(Vec<Message>, Vec<Vec<u8>>), NodeError> {
        if !self.raw_node.has_ready() {
            return Ok((vec![], vec![]));
        }

        let mut ready = self.raw_node.ready();

        if !ready.entries().is_empty() {
            self.storage.append(ready.entries())?;
        }

        if let Some(hs) = ready.hs() {
            self.storage.set_hard_state(hs.clone());
        }

        let messages = ready.take_messages();
        let mut committed_payloads = Vec::new();
        for entry in ready.committed_entries().iter() {
            if !entry.get_data().is_empty() {
                committed_payloads.push(entry.get_data().to_vec());
            }
        }

        self.raw_node.advance(ready);

        Ok((messages, committed_payloads))
    }
}

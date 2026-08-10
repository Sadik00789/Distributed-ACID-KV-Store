use crate::server::NodeState;
use protobuf::Message as PbMessage;
use proto::kv::kv_service_server::KvService;
use proto::kv::{
    AskSplitRequest, AskSplitResponse, BatchCommitRequest, BatchCommitResponse,
    BatchPrewriteRequest, BatchPrewriteResponse, GetRequest as KvGetRequest,
    GetResponse as KvGetResponse, KeyValue, RegionStatsRequest, RegionStatsResponse,
    ScanRequest, ScanResponse, TxnRequest, TxnResponse,
};
use proto::raft::raft_service_server::RaftService;
use proto::raft::{RaftMessage, RaftResponse, SnapshotChunk, SnapshotResponse};
use proto::txn::txn_service_server::TxnService;
use proto::txn::{
    CheckTxnStatusRequest, CheckTxnStatusResponse, CommitRequest, CommitResponse, PrewriteRequest,
    PrewriteResponse, RollbackRequest, RollbackResponse, TxnHeartBeatRequest, TxnHeartBeatResponse,
};
use raft::eraftpb::Message as EraftMessage;
use raft_engine::{MultiRaftNode, RaftConfig};
use tonic::{Request, Response, Status};
use txn_coordinator::{Mutation, TxnStatus};

// --- KV SERVICE ---
pub struct KvServiceImpl {
    node: NodeState,
}

impl KvServiceImpl {
    pub fn new(node: NodeState) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl KvService for KvServiceImpl {
    async fn get(&self, request: Request<KvGetRequest>) -> Result<Response<KvGetResponse>, Status> {
        let req = request.into_inner();

        if let Ok(region) = self.node.router.route_key(&req.key) {
            if !region.contains_key(&req.key) {
                return Err(Status::out_of_range("ErrKeyNotInRegion"));
            }
        }

        let read_ts = if req.version == 0 {
            self.node.hlc.now()
        } else {
            req.version
        };

        match self.node.coordinator.get(&req.key, read_ts) {
            Ok(value) => Ok(Response::new(KvGetResponse {
                found: value.is_some(),
                value: value.unwrap_or_default(),
                error: String::new(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        let req = request.into_inner();
        let read_ts = if req.version == 0 {
            self.node.hlc.now()
        } else {
            req.version
        };

        match self
            .node
            .coordinator
            .scan(&req.start_key, &req.end_key, req.limit, read_ts)
        {
            Ok(kvs) => Ok(Response::new(ScanResponse {
                kvs: kvs
                    .into_iter()
                    .map(|(key, value)| KeyValue { key, value })
                    .collect(),
                error: String::new(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn execute_txn(
        &self,
        _request: Request<TxnRequest>,
    ) -> Result<Response<TxnResponse>, Status> {
        Err(Status::unimplemented(
            "Direct multi-key transaction execution endpoint",
        ))
    }

    async fn batch_prewrite(
        &self,
        request: Request<BatchPrewriteRequest>,
    ) -> Result<Response<BatchPrewriteResponse>, Status> {
        let req = request.into_inner();
        let mutations: Vec<Mutation> = req
            .mutations
            .into_iter()
            .map(|m| {
                if m.op == 1 {
                    Mutation::Delete { key: m.key }
                } else {
                    Mutation::Put {
                        key: m.key,
                        value: m.value,
                    }
                }
            })
            .collect();

        match self.node.coordinator.batch_prewrite(
            req.start_ts,
            &mutations,
            &req.primary_lock,
            req.lock_ttl,
        ) {
            Ok(_) => Ok(Response::new(BatchPrewriteResponse { errors: vec![] })),
            Err(e) => Err(Status::aborted(e.to_string())),
        }
    }

    async fn batch_commit(
        &self,
        request: Request<BatchCommitRequest>,
    ) -> Result<Response<BatchCommitResponse>, Status> {
        let req = request.into_inner();
        match self
            .node
            .coordinator
            .batch_commit(req.start_ts, req.commit_ts, &req.keys)
        {
            Ok(_) => Ok(Response::new(BatchCommitResponse { error: None })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn ask_split(
        &self,
        request: Request<AskSplitRequest>,
    ) -> Result<Response<AskSplitResponse>, Status> {
        let req = request.into_inner();
        let region = match self.node.router.get_region(req.region_id) {
            Some(r) => r,
            None => {
                return Ok(Response::new(AskSplitResponse {
                    new_region_id: 0,
                    left_start_key: vec![],
                    left_end_key: vec![],
                    right_start_key: vec![],
                    right_end_key: vec![],
                    error: format!("Region {} not found", req.region_id),
                }));
            }
        };

        let split_key = if !req.split_key.is_empty() {
            req.split_key
        } else {
            match self
                .node
                .storage
                .find_median_key(&region.start_key, &region.end_key)
            {
                Ok(Some(k)) => k,
                _ => {
                    return Ok(Response::new(AskSplitResponse {
                        new_region_id: 0,
                        left_start_key: vec![],
                        left_end_key: vec![],
                        right_start_key: vec![],
                        right_end_key: vec![],
                        error: "Could not find split key".to_string(),
                    }));
                }
            }
        };

        let new_region_id = self.node.alloc_region_id();
        match self
            .node
            .router
            .split_region(req.region_id, split_key.clone(), new_region_id)
        {
            Ok((left, right)) => {
                let raft_cfg = RaftConfig {
                    node_id: self.node.config.node_id,
                    ..Default::default()
                };
                if let Ok(new_node) = MultiRaftNode::new(
                    new_region_id,
                    vec![self.node.config.node_id],
                    &raft_cfg,
                    self.node.storage.clone(),
                ) {
                    self.node.raft_nodes.lock().insert(new_region_id, new_node);
                }

                Ok(Response::new(AskSplitResponse {
                    new_region_id,
                    left_start_key: left.start_key,
                    left_end_key: left.end_key,
                    right_start_key: right.start_key,
                    right_end_key: right.end_key,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(AskSplitResponse {
                new_region_id: 0,
                left_start_key: vec![],
                left_end_key: vec![],
                right_start_key: vec![],
                right_end_key: vec![],
                error: e.to_string(),
            })),
        }
    }

    async fn report_region_stats(
        &self,
        request: Request<RegionStatsRequest>,
    ) -> Result<Response<RegionStatsResponse>, Status> {
        let req = request.into_inner();
        let region = match self.node.router.get_region(req.region_id) {
            Some(r) => r,
            None => {
                return Ok(Response::new(RegionStatsResponse {
                    split_recommended: false,
                    suggested_split_key: vec![],
                }));
            }
        };

        let (key_count, total_bytes) = self
            .node
            .storage
            .calculate_region_stats(&region.start_key, &region.end_key)
            .unwrap_or((0, 0));

        let split_recommended = key_count >= 10000 || total_bytes >= 64 * 1024 * 1024;
        let suggested_split_key = if split_recommended {
            self.node
                .storage
                .find_median_key(&region.start_key, &region.end_key)
                .unwrap_or(None)
                .unwrap_or_default()
        } else {
            vec![]
        };

        Ok(Response::new(RegionStatsResponse {
            split_recommended,
            suggested_split_key,
        }))
    }
}

// --- TXN SERVICE ---
pub struct TxnServiceImpl {
    node: NodeState,
}

impl TxnServiceImpl {
    pub fn new(node: NodeState) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl TxnService for TxnServiceImpl {
    async fn prewrite(
        &self,
        request: Request<PrewriteRequest>,
    ) -> Result<Response<PrewriteResponse>, Status> {
        let req = request.into_inner();
        let mutations: Vec<Mutation> = req
            .mutations
            .into_iter()
            .map(|m| {
                if m.op == 1 {
                    Mutation::Delete { key: m.key }
                } else {
                    Mutation::Put {
                        key: m.key,
                        value: m.value,
                    }
                }
            })
            .collect();

        match self.node.coordinator.prewrite(
            req.start_ts,
            &mutations,
            &req.primary_lock,
            req.lock_ttl,
        ) {
            Ok(_) => Ok(Response::new(PrewriteResponse { errors: vec![] })),
            Err(e) => Err(Status::aborted(e.to_string())),
        }
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let req = request.into_inner();
        match self
            .node
            .coordinator
            .commit(req.start_ts, req.commit_ts, &req.keys)
        {
            Ok(_) => Ok(Response::new(CommitResponse { error: None })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        let req = request.into_inner();
        match self.node.coordinator.rollback(req.start_ts, &req.keys) {
            Ok(_) => Ok(Response::new(RollbackResponse { error: None })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn check_txn_status(
        &self,
        request: Request<CheckTxnStatusRequest>,
    ) -> Result<Response<CheckTxnStatusResponse>, Status> {
        let req = request.into_inner();
        match self
            .node
            .coordinator
            .check_txn_status(&req.primary_key, req.lock_ts)
        {
            Ok(status) => match status {
                TxnStatus::Locked { ttl } => Ok(Response::new(CheckTxnStatusResponse {
                    lock_ttl: ttl,
                    commit_ts: 0,
                    is_rolled_back: false,
                    error: String::new(),
                })),
                TxnStatus::Committed { commit_ts } => Ok(Response::new(CheckTxnStatusResponse {
                    lock_ttl: 0,
                    commit_ts,
                    is_rolled_back: false,
                    error: String::new(),
                })),
                TxnStatus::RolledBack => Ok(Response::new(CheckTxnStatusResponse {
                    lock_ttl: 0,
                    commit_ts: 0,
                    is_rolled_back: true,
                    error: String::new(),
                })),
            },
            Err(e) => Ok(Response::new(CheckTxnStatusResponse {
                lock_ttl: 0,
                commit_ts: 0,
                is_rolled_back: false,
                error: e.to_string(),
            })),
        }
    }

    async fn heart_beat(
        &self,
        request: Request<TxnHeartBeatRequest>,
    ) -> Result<Response<TxnHeartBeatResponse>, Status> {
        let req = request.into_inner();
        match self
            .node
            .coordinator
            .heartbeat(&req.primary_lock, req.start_ts, req.ttl)
        {
            Ok(ttl) => Ok(Response::new(TxnHeartBeatResponse {
                success: true,
                ttl,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(TxnHeartBeatResponse {
                success: false,
                ttl: 0,
                error: e.to_string(),
            })),
        }
    }
}

// --- RAFT SERVICE ---
pub struct RaftServiceImpl {
    node: NodeState,
}

impl RaftServiceImpl {
    pub fn new(node: NodeState) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn send_message(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftResponse>, Status> {
        let msg = request.into_inner();
        if let Ok(eraft_msg) = EraftMessage::parse_from_bytes(&msg.message_data) {
            let mut nodes = self.node.raft_nodes.lock();
            if let Some(raft_node) = nodes.get_mut(&msg.region_id) {
                let _ = raft_node.step(eraft_msg);
            }
        }

        Ok(Response::new(RaftResponse {
            accepted: true,
            error: String::new(),
        }))
    }

    async fn stream_snapshot(
        &self,
        _request: Request<tonic::Streaming<SnapshotChunk>>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        Ok(Response::new(SnapshotResponse {
            success: true,
            error: String::new(),
        }))
    }

    async fn step(
        &self,
        request: Request<tonic::Streaming<RaftMessage>>,
    ) -> Result<Response<RaftResponse>, Status> {
        let mut stream = request.into_inner();
        while let Ok(Some(msg)) = stream.message().await {
            if let Ok(eraft_msg) = EraftMessage::parse_from_bytes(&msg.message_data) {
                let mut nodes = self.node.raft_nodes.lock();
                if let Some(raft_node) = nodes.get_mut(&msg.region_id) {
                    let _ = raft_node.step(eraft_msg);
                }
            }
        }

        Ok(Response::new(RaftResponse {
            accepted: true,
            error: String::new(),
        }))
    }
}

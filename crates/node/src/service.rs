use crate::server::NodeState;
use proto::kv::kv_service_server::KvService;
use proto::kv::{
    GetRequest as KvGetRequest, GetResponse as KvGetResponse, KeyValue, ScanRequest, ScanResponse,
    TxnRequest, TxnResponse,
};
use proto::raft::raft_service_server::RaftService;
use proto::raft::{RaftMessage, RaftResponse, SnapshotChunk, SnapshotResponse};
use proto::txn::txn_service_server::TxnService;
use proto::txn::{
    CheckTxnStatusRequest, CheckTxnStatusResponse, CommitRequest, CommitResponse, PrewriteRequest,
    PrewriteResponse, RollbackRequest, RollbackResponse, TxnHeartBeatRequest, TxnHeartBeatResponse,
};
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

        // Evaluates req.version for MVCC reads: version == 0 fetches latest HLC time,
        // otherwise reads strictly at the specified transaction version/timestamp.
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
    _node: NodeState,
}

impl RaftServiceImpl {
    pub fn new(node: NodeState) -> Self {
        Self { _node: node }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn send_message(
        &self,
        _request: Request<RaftMessage>,
    ) -> Result<Response<RaftResponse>, Status> {
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
}

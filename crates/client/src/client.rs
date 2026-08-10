use proto::kv::kv_service_client::KvServiceClient as RawKvClient;
use proto::kv::{GetRequest, ScanRequest};
use proto::txn::txn_service_client::TxnServiceClient as RawTxnClient;
use proto::txn::{
    CommitRequest, Mutation as ProtoMutation, PrewriteRequest, RollbackRequest, TxnHeartBeatRequest,
};
use thiserror::Error;
use tonic::transport::Channel;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("gRPC transport error: {0}")]
    Tonic(#[from] tonic::transport::Error),
    #[error("URI parse error: {0}")]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("RPC status error: {0}")]
    Status(#[from] tonic::Status),
    #[error("Key not found")]
    NotFound,
}

#[derive(Clone)]
pub struct KvClient {
    kv_inner: RawKvClient<Channel>,
    txn_inner: RawTxnClient<Channel>,
}

impl KvClient {
    pub async fn connect(endpoint: String) -> Result<Self, ClientError> {
        let channel = Channel::from_shared(endpoint)?.connect().await?;
        Ok(Self {
            kv_inner: RawKvClient::new(channel.clone()),
            txn_inner: RawTxnClient::new(channel),
        })
    }

    /// Point-in-time snapshot read (uses current clock timestamp).
    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ClientError> {
        let res = self
            .kv_inner
            .get(GetRequest { key, version: 0 })
            .await?
            .into_inner();
        if res.found {
            Ok(Some(res.value))
        } else {
            Ok(None)
        }
    }

    /// Range scan over keys in [start_key, end_key) at current snapshot version.
    pub async fn scan(
        &mut self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ClientError> {
        let res = self
            .kv_inner
            .scan(ScanRequest {
                start_key,
                end_key,
                limit,
                version: 0,
            })
            .await?
            .into_inner();

        Ok(res.kvs.into_iter().map(|kv| (kv.key, kv.value)).collect())
    }

    /// Transactional MVCC read at a specific transaction `start_ts`.
    pub async fn txn_get(
        &mut self,
        key: Vec<u8>,
        start_ts: u64,
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let res = self
            .kv_inner
            .get(GetRequest {
                key,
                version: start_ts,
            })
            .await?
            .into_inner();
        if res.found {
            Ok(Some(res.value))
        } else {
            Ok(None)
        }
    }

    pub async fn txn_prewrite(
        &mut self,
        start_ts: u64,
        mutations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        primary_key: Vec<u8>,
        lock_ttl: u64,
    ) -> Result<(), ClientError> {
        let proto_mutations = mutations
            .into_iter()
            .map(|(key, value)| match value {
                Some(val) => ProtoMutation {
                    op: 0,
                    key,
                    value: val,
                },
                None => ProtoMutation {
                    op: 1,
                    key,
                    value: vec![],
                },
            })
            .collect();

        self.txn_inner
            .prewrite(PrewriteRequest {
                start_ts,
                mutations: proto_mutations,
                primary_lock: primary_key,
                lock_ttl,
            })
            .await?;

        Ok(())
    }

    pub async fn txn_commit(
        &mut self,
        start_ts: u64,
        commit_ts: u64,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), ClientError> {
        self.txn_inner
            .commit(CommitRequest {
                start_ts,
                commit_ts,
                keys,
            })
            .await?;
        Ok(())
    }

    pub async fn txn_rollback(
        &mut self,
        start_ts: u64,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), ClientError> {
        self.txn_inner
            .rollback(RollbackRequest { start_ts, keys })
            .await?;
        Ok(())
    }

    pub async fn txn_heartbeat(
        &mut self,
        start_ts: u64,
        primary_key: Vec<u8>,
        ttl: u64,
    ) -> Result<bool, ClientError> {
        let res = self
            .txn_inner
            .heart_beat(TxnHeartBeatRequest {
                start_ts,
                primary_lock: primary_key,
                ttl,
            })
            .await?
            .into_inner();
        Ok(res.success)
    }
}

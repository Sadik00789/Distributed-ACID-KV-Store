use proto::kv::kv_service_client::KvServiceClient as RawKvClient;
use proto::kv::{
    AskSplitRequest, AskSplitResponse, BatchCommitRequest, BatchPrewriteRequest, GetRequest,
    Mutation as KvMutation, ScanRequest,
};
use proto::txn::txn_service_client::TxnServiceClient as RawTxnClient;
use proto::txn::{
    CommitRequest, Mutation as TxnMutation, PrewriteRequest, RollbackRequest, TxnHeartBeatRequest,
};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
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
    #[error("Batch channel closed")]
    ChannelClosed,
    #[error("Region split error: {0}")]
    SplitError(String),
}

struct PrewriteItem {
    start_ts: u64,
    mutations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    primary_key: Vec<u8>,
    lock_ttl: u64,
    tx: oneshot::Sender<Result<(), ClientError>>,
}

struct CommitItem {
    start_ts: u64,
    commit_ts: u64,
    keys: Vec<Vec<u8>>,
    tx: oneshot::Sender<Result<(), ClientError>>,
}

#[derive(Clone)]
pub struct BatchCollector {
    prewrite_tx: mpsc::Sender<PrewriteItem>,
    commit_tx: mpsc::Sender<CommitItem>,
}

impl BatchCollector {
    pub fn new(client: KvClient, batch_size: usize, flush_interval_ms: u64) -> Self {
        let (pw_tx, mut pw_rx) = mpsc::channel::<PrewriteItem>(1024);
        let (cm_tx, mut cm_rx) = mpsc::channel::<CommitItem>(1024);

        let mut client_pw = client.clone();
        tokio::spawn(async move {
            let interval = Duration::from_millis(flush_interval_ms);
            loop {
                let mut items = Vec::new();
                let timeout = tokio::time::sleep(interval);
                tokio::pin!(timeout);

                loop {
                    tokio::select! {
                        item = pw_rx.recv() => {
                            match item {
                                Some(it) => {
                                    items.push(it);
                                    if items.len() >= batch_size {
                                        break;
                                    }
                                }
                                None => return,
                            }
                        }
                        _ = &mut timeout => {
                            break;
                        }
                    }
                }

                if !items.is_empty() {
                    for item in items {
                        let res = client_pw
                            .batch_prewrite(
                                item.start_ts,
                                item.mutations,
                                item.primary_key,
                                item.lock_ttl,
                            )
                            .await;
                        let _ = item.tx.send(res);
                    }
                }
            }
        });

        let mut client_cm = client;
        tokio::spawn(async move {
            let interval = Duration::from_millis(flush_interval_ms);
            loop {
                let mut items = Vec::new();
                let timeout = tokio::time::sleep(interval);
                tokio::pin!(timeout);

                loop {
                    tokio::select! {
                        item = cm_rx.recv() => {
                            match item {
                                Some(it) => {
                                    items.push(it);
                                    if items.len() >= batch_size {
                                        break;
                                    }
                                }
                                None => return,
                            }
                        }
                        _ = &mut timeout => {
                            break;
                        }
                    }
                }

                if !items.is_empty() {
                    for item in items {
                        let res = client_cm
                            .batch_commit(item.start_ts, item.commit_ts, item.keys)
                            .await;
                        let _ = item.tx.send(res);
                    }
                }
            }
        });

        Self {
            prewrite_tx: pw_tx,
            commit_tx: cm_tx,
        }
    }

    pub async fn submit_prewrite(
        &self,
        start_ts: u64,
        mutations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        primary_key: Vec<u8>,
        lock_ttl: u64,
    ) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        self.prewrite_tx
            .send(PrewriteItem {
                start_ts,
                mutations,
                primary_key,
                lock_ttl,
                tx,
            })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;
        rx.await.map_err(|_| ClientError::ChannelClosed)?
    }

    pub async fn submit_commit(
        &self,
        start_ts: u64,
        commit_ts: u64,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        self.commit_tx
            .send(CommitItem {
                start_ts,
                commit_ts,
                keys,
                tx,
            })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;
        rx.await.map_err(|_| ClientError::ChannelClosed)?
    }
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
                Some(val) => TxnMutation {
                    op: 0,
                    key,
                    value: val,
                },
                None => TxnMutation {
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

    pub async fn batch_prewrite(
        &mut self,
        start_ts: u64,
        mutations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        primary_key: Vec<u8>,
        lock_ttl: u64,
    ) -> Result<(), ClientError> {
        let proto_mutations = mutations
            .into_iter()
            .map(|(key, value)| match value {
                Some(val) => KvMutation {
                    op: 0,
                    key,
                    value: val,
                },
                None => KvMutation {
                    op: 1,
                    key,
                    value: vec![],
                },
            })
            .collect();

        self.kv_inner
            .batch_prewrite(BatchPrewriteRequest {
                start_ts,
                primary_lock: primary_key,
                lock_ttl,
                mutations: proto_mutations,
            })
            .await?;

        Ok(())
    }

    pub async fn batch_commit(
        &mut self,
        start_ts: u64,
        commit_ts: u64,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), ClientError> {
        self.kv_inner
            .batch_commit(BatchCommitRequest {
                start_ts,
                commit_ts,
                keys,
            })
            .await?;

        Ok(())
    }

    pub async fn ask_split(
        &mut self,
        region_id: u64,
        split_key: Vec<u8>,
    ) -> Result<AskSplitResponse, ClientError> {
        let res = self
            .kv_inner
            .ask_split(AskSplitRequest {
                region_id,
                split_key,
            })
            .await?
            .into_inner();
        Ok(res)
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

use crate::hlc::HybridLogicalClock;
use crate::lock_resolver::{LockResolver, LockResolverError, TxnStatus};
use storage::{KeyEncoder, Lock, MvccError, MvccReader, OpType, StorageEngine, WriteRecord};
use thiserror::Error;
use tracing::info;

#[derive(Error, Debug)]
pub enum TxnError {
    #[error("Storage error: {0}")]
    Storage(#[from] storage::StorageError),
    #[error("Fjall engine error: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("MVCC error: {0}")]
    Mvcc(#[from] MvccError),
    #[error("Lock resolver error: {0}")]
    LockResolver(#[from] LockResolverError),
    #[error("Write conflict detected for key: {0:?} at start_ts: {1}")]
    WriteConflict(Vec<u8>, u64),
    #[error("Key is locked: {0:?}")]
    KeyLocked(Vec<u8>),
    #[error("Transaction already committed or aborted")]
    TxnClosed,
    #[error("Transaction was rolled back")]
    TransactionRolledBack,
}

#[derive(Debug, Clone)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Mutation {
    pub fn key(&self) -> &[u8] {
        match self {
            Mutation::Put { key, .. } => key,
            Mutation::Delete { key } => key,
        }
    }

    pub fn op_type(&self) -> OpType {
        match self {
            Mutation::Put { .. } => OpType::Put,
            Mutation::Delete { .. } => OpType::Delete,
        }
    }
}

pub struct TransactionCoordinator {
    engine: StorageEngine,
    hlc: HybridLogicalClock,
    resolver: LockResolver,
}

impl TransactionCoordinator {
    pub fn new(engine: StorageEngine, hlc: HybridLogicalClock) -> Self {
        let resolver = LockResolver::new(engine.clone());
        Self {
            engine,
            hlc,
            resolver,
        }
    }

    pub fn begin(&self) -> u64 {
        self.hlc.now()
    }

    /// Queries transaction status for a primary key at start_ts.
    pub fn check_txn_status(
        &self,
        primary_key: &[u8],
        start_ts: u64,
    ) -> Result<TxnStatus, TxnError> {
        self.resolver
            .check_txn_status(primary_key, start_ts)
            .map_err(TxnError::LockResolver)
    }

    /// Extends TTL on an active primary lock if start_ts matches.
    pub fn heartbeat(
        &self,
        primary_key: &[u8],
        start_ts: u64,
        new_ttl: u64,
    ) -> Result<u64, TxnError> {
        let lock_p = self.engine.lock_partition();
        if let Some(lock_bytes) = lock_p.get(primary_key)? {
            let mut lock = Lock::from_bytes(&lock_bytes)
                .map_err(|_| TxnError::Mvcc(MvccError::CorruptedState))?;

            if lock.start_ts == start_ts {
                lock.ttl = new_ttl;
                let mut batch = self.engine.batch();
                batch.insert(lock_p, primary_key, lock.to_bytes());
                self.engine.commit_batch(batch)?;
                return Ok(lock.ttl);
            }
        }
        Err(TxnError::TransactionRolledBack)
    }

    /// Reads value at `read_ts`. Resolves stale locks automatically when encountered.
    pub fn get(&self, user_key: &[u8], read_ts: u64) -> Result<Option<Vec<u8>>, TxnError> {
        let reader = MvccReader::new(&self.engine);
        match reader.get(user_key, read_ts) {
            Ok(val) => Ok(val),
            Err(MvccError::KeyIsLocked {
                primary_key,
                lock_ts,
            }) => {
                let lock = Lock {
                    primary_key,
                    start_ts: lock_ts,
                    ttl: 3000,
                    op: OpType::Put,
                };
                match self.resolver.resolve_lock(user_key, &lock) {
                    Ok(_) => Ok(reader.get(user_key, read_ts)?),
                    Err(_) => Err(TxnError::KeyLocked(user_key.to_vec())),
                }
            }
            Err(e) => Err(TxnError::Mvcc(e)),
        }
    }

    /// Scans keys in range [start_key, end_key) at snapshot timestamp read_ts.
    pub fn scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: u32,
        read_ts: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnError> {
        let write_p = self.engine.write_partition();
        let seek_key = if start_key.is_empty() {
            vec![]
        } else {
            KeyEncoder::encode(start_key, read_ts)
        };

        let mut results = Vec::new();
        let mut last_seen_user_key: Option<Vec<u8>> = None;

        for item in write_p.range(seek_key..) {
            let (enc_key, _) = item?;
            let (user_key, _) = KeyEncoder::decode(&enc_key)
                .map_err(|_| TxnError::Mvcc(MvccError::CorruptedState))?;

            if !start_key.is_empty() && user_key < start_key {
                continue;
            }
            if !end_key.is_empty() && user_key >= end_key {
                break;
            }
            if last_seen_user_key.as_deref() == Some(user_key) {
                continue;
            }

            last_seen_user_key = Some(user_key.to_vec());

            if let Some(value) = self.get(user_key, read_ts)? {
                results.push((user_key.to_vec(), value));
                if limit > 0 && results.len() >= limit as usize {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Phase 1: Prewrite mutations. First mutation in slice acts as Primary key.
    pub fn prewrite(
        &self,
        start_ts: u64,
        mutations: &[Mutation],
        primary_key: &[u8],
        lock_ttl: u64,
    ) -> Result<(), TxnError> {
        if mutations.is_empty() {
            return Ok(());
        }

        let lock_p = self.engine.lock_partition();
        let default_p = self.engine.default_partition();
        let write_p = self.engine.write_partition();

        for mutation in mutations {
            let key = mutation.key();

            // 1. Check write conflict & permanent rollback record
            let seek_key = KeyEncoder::encode(key, u64::MAX);
            for item in write_p.range(seek_key..) {
                let (enc_key, write_bytes) = item?;
                let (curr_user_key, commit_ts) = KeyEncoder::decode(&enc_key)
                    .map_err(|_| TxnError::Mvcc(MvccError::CorruptedState))?;

                if curr_user_key == key {
                    if let Some(rec) = WriteRecord::from_bytes(&write_bytes) {
                        if rec.start_ts == start_ts && rec.op == OpType::Rollback {
                            return Err(TxnError::TransactionRolledBack);
                        }
                    }
                    if commit_ts >= start_ts {
                        return Err(TxnError::WriteConflict(key.to_vec(), start_ts));
                    }
                } else {
                    break;
                }
            }

            // 2. Check active lock and attempt resolution if needed
            if let Some(existing_lock_bytes) = lock_p.get(key)? {
                let lock = Lock::from_bytes(&existing_lock_bytes)
                    .map_err(|_| TxnError::Mvcc(MvccError::CorruptedState))?;
                if lock.start_ts != start_ts {
                    if self.resolver.resolve_lock(key, &lock).is_err() {
                        return Err(TxnError::KeyLocked(key.to_vec()));
                    }
                    if let Some(still_locked) = lock_p.get(key)? {
                        let l = Lock::from_bytes(&still_locked)
                            .map_err(|_| TxnError::Mvcc(MvccError::CorruptedState))?;
                        if l.start_ts != start_ts {
                            return Err(TxnError::KeyLocked(key.to_vec()));
                        }
                    }
                }
            }

            // 3. Write value to default partition and register lock
            let mut batch = self.engine.batch();
            if let Mutation::Put { key, value } = mutation {
                let data_key = KeyEncoder::encode(key, start_ts);
                batch.insert(default_p, data_key, value.clone());
            }

            let lock = Lock {
                primary_key: primary_key.to_vec(),
                start_ts,
                ttl: lock_ttl,
                op: mutation.op_type(),
            };
            batch.insert(lock_p, key, lock.to_bytes());

            self.engine.commit_batch(batch)?;
        }

        Ok(())
    }

    /// Phase 2: Commit transaction. Commits primary key first, then secondaries.
    pub fn commit(&self, start_ts: u64, commit_ts: u64, keys: &[Vec<u8>]) -> Result<(), TxnError> {
        if keys.is_empty() {
            return Ok(());
        }

        let primary_key = &keys[0];
        let lock_p = self.engine.lock_partition();
        let write_p = self.engine.write_partition();

        // Step 1: Commit Primary Key
        if let Some(lock_bytes) = lock_p.get(primary_key)? {
            let lock = Lock::from_bytes(&lock_bytes)
                .map_err(|_| TxnError::Mvcc(MvccError::CorruptedState))?;

            if lock.start_ts == start_ts {
                let mut batch = self.engine.batch();
                let write_key = KeyEncoder::encode(primary_key, commit_ts);
                let write_rec = WriteRecord::new(start_ts, lock.op);

                batch.insert(write_p, write_key, write_rec.to_bytes());
                batch.remove(lock_p, primary_key);
                self.engine.commit_batch(batch)?;
                info!(primary_key = ?primary_key, commit_ts, "Committed primary key");
            } else {
                return Err(TxnError::TransactionRolledBack);
            }
        } else {
            return Err(TxnError::TransactionRolledBack);
        }

        // Step 2: Commit Secondary Keys
        for key in &keys[1..] {
            if let Some(lock_bytes) = lock_p.get(key)? {
                if let Ok(lock) = Lock::from_bytes(&lock_bytes) {
                    if lock.start_ts == start_ts {
                        let mut batch = self.engine.batch();
                        let write_key = KeyEncoder::encode(key, commit_ts);
                        let write_rec = WriteRecord::new(start_ts, lock.op);

                        batch.insert(write_p, write_key, write_rec.to_bytes());
                        batch.remove(lock_p, key);
                        self.engine.commit_batch(batch)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Rollback transaction across specified keys.
    pub fn rollback(&self, start_ts: u64, keys: &[Vec<u8>]) -> Result<(), TxnError> {
        let lock_p = self.engine.lock_partition();
        let write_p = self.engine.write_partition();

        for key in keys {
            let mut batch = self.engine.batch();
            if let Some(lock_bytes) = lock_p.get(key)? {
                if let Ok(lock) = Lock::from_bytes(&lock_bytes) {
                    if lock.start_ts == start_ts {
                        batch.remove(lock_p, key);
                    }
                }
            }
            let write_key = KeyEncoder::encode(key, start_ts);
            let write_rec = WriteRecord::new(start_ts, OpType::Rollback);
            batch.insert(write_p, write_key, write_rec.to_bytes());
            self.engine.commit_batch(batch)?;
        }

        Ok(())
    }
}

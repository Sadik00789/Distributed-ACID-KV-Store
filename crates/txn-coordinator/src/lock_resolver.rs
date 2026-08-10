use std::time::{SystemTime, UNIX_EPOCH};
use storage::{KeyEncoder, Lock, MvccError, OpType, StorageEngine, WriteRecord};
use thiserror::Error;
use tracing::info;

#[derive(Error, Debug)]
pub enum LockResolverError {
    #[error("Storage error: {0}")]
    Storage(#[from] storage::StorageError),
    #[error("Fjall engine error: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("MVCC error: {0}")]
    Mvcc(#[from] MvccError),
    #[error("Lock belongs to active transaction and is not expired")]
    LockActiveNotExpired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnStatus {
    Committed { commit_ts: u64 },
    RolledBack,
    Locked { ttl: u64 },
}

#[derive(Clone)]
pub struct LockResolver {
    engine: StorageEngine,
}

impl LockResolver {
    pub fn new(engine: StorageEngine) -> Self {
        Self { engine }
    }

    /// Queries the status of a primary lock at start_ts.
    pub fn check_txn_status(
        &self,
        primary_key: &[u8],
        start_ts: u64,
    ) -> Result<TxnStatus, LockResolverError> {
        let lock_p = self.engine.lock_partition();
        let write_p = self.engine.write_partition();

        // 1. Scan write partition to verify if committed or rolled back
        let seek_key = KeyEncoder::encode(primary_key, u64::MAX);
        for item in write_p.range(seek_key..) {
            let (enc_key, write_bytes) = item?;
            let (curr_user_key, commit_ts) = KeyEncoder::decode(&enc_key)
                .map_err(|_| LockResolverError::Mvcc(MvccError::CorruptedState))?;

            if curr_user_key == primary_key {
                if let Some(record) = WriteRecord::from_bytes(&write_bytes) {
                    if record.start_ts == start_ts {
                        match record.op {
                            OpType::Put | OpType::Delete => {
                                return Ok(TxnStatus::Committed { commit_ts });
                            }
                            OpType::Rollback => {
                                return Ok(TxnStatus::RolledBack);
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }

        // 2. Check lock partition for active primary lock
        if let Some(lock_bytes) = lock_p.get(primary_key)? {
            if let Ok(lock) = Lock::from_bytes(&lock_bytes) {
                if lock.start_ts == start_ts {
                    let start_phys = start_ts >> 16;
                    let now_phys = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    if now_phys > start_phys + lock.ttl {
                        info!(primary_key = ?primary_key, start_ts, "Primary lock TTL expired; rolling back primary");
                        let mut batch = self.engine.batch();
                        let write_key = KeyEncoder::encode(primary_key, start_ts);
                        let write_rec = WriteRecord::new(start_ts, OpType::Rollback);
                        batch.insert(write_p, write_key, write_rec.to_bytes());
                        batch.remove(lock_p, primary_key);
                        self.engine.commit_batch(batch)?;

                        return Ok(TxnStatus::RolledBack);
                    }

                    return Ok(TxnStatus::Locked { ttl: lock.ttl });
                }
            }
        }

        // Lock absent and no commit record => rolled back or non-existent
        Ok(TxnStatus::RolledBack)
    }

    /// Resolves a secondary lock on `key` by rolling forward (committing) or rolling back.
    pub fn resolve_lock(&self, key: &[u8], lock: &Lock) -> Result<(), LockResolverError> {
        let status = self.check_txn_status(&lock.primary_key, lock.start_ts)?;

        let mut batch = self.engine.batch();
        let lock_p = self.engine.lock_partition();
        let write_p = self.engine.write_partition();

        match status {
            TxnStatus::Committed { commit_ts } => {
                info!(key = ?key, start_ts = lock.start_ts, commit_ts, "Rolling forward secondary key");
                let write_key = KeyEncoder::encode(key, commit_ts);
                let write_rec = WriteRecord::new(lock.start_ts, lock.op);
                batch.insert(write_p, write_key, write_rec.to_bytes());
                batch.remove(lock_p, key);
                self.engine.commit_batch(batch)?;
            }
            TxnStatus::RolledBack => {
                info!(key = ?key, start_ts = lock.start_ts, "Rolling back secondary key");
                let write_key = KeyEncoder::encode(key, lock.start_ts);
                let write_rec = WriteRecord::new(lock.start_ts, OpType::Rollback);
                batch.insert(write_p, write_key, write_rec.to_bytes());
                batch.remove(lock_p, key);
                self.engine.commit_batch(batch)?;
            }
            TxnStatus::Locked { .. } => {
                let start_phys = lock.start_ts >> 16;
                let now_phys = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                if now_phys > start_phys + lock.ttl {
                    info!(key = ?key, start_ts = lock.start_ts, "Secondary lock TTL expired; rolling back");
                    let write_key = KeyEncoder::encode(key, lock.start_ts);
                    let write_rec = WriteRecord::new(lock.start_ts, OpType::Rollback);
                    batch.insert(write_p, write_key, write_rec.to_bytes());
                    batch.remove(lock_p, key);
                    self.engine.commit_batch(batch)?;
                } else {
                    return Err(LockResolverError::LockActiveNotExpired);
                }
            }
        }

        Ok(())
    }
}

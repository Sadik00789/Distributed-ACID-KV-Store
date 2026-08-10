use crate::engine::StorageEngine;
use crate::lock::Lock;
use crate::mvcc::{KeyEncoder, OpType, WriteRecord};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MvccError {
    #[error("Storage engine error: {0}")]
    Engine(#[from] crate::engine::StorageError),
    #[error("Fjall storage error: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("Key is locked by active transaction: primary_key={primary_key:?}, lock_ts={lock_ts}")]
    KeyIsLocked { primary_key: Vec<u8>, lock_ts: u64 },
    #[error("Corrupted value state in storage")]
    CorruptedState,
}

pub struct MvccReader<'a> {
    engine: &'a StorageEngine,
}

impl<'a> MvccReader<'a> {
    pub fn new(engine: &'a StorageEngine) -> Self {
        Self { engine }
    }

    /// Reads value of user_key at read_ts. Fails if locked by an active transaction with start_ts <= read_ts.
    pub fn get(&self, user_key: &[u8], read_ts: u64) -> Result<Option<Vec<u8>>, MvccError> {
        let lock_p = self.engine.lock_partition();
        let write_p = self.engine.write_partition();
        let default_p = self.engine.default_partition();

        // 1. Check for active uncommitted locks in lock partition
        if let Some(lock_bytes) = lock_p.get(user_key)? {
            let lock = Lock::from_bytes(&lock_bytes).map_err(|_| MvccError::CorruptedState)?;
            if lock.start_ts <= read_ts {
                return Err(MvccError::KeyIsLocked {
                    primary_key: lock.primary_key,
                    lock_ts: lock.start_ts,
                });
            }
        }

        // 2. Scan write partition for most recent commit record at or before read_ts
        let seek_key = KeyEncoder::encode(user_key, read_ts);

        // Range scan starting from seek_key forward
        for item in write_p.range(seek_key..) {
            let (enc_key, write_bytes) = item?;
            let (curr_user_key, commit_ts) =
                KeyEncoder::decode(&enc_key).map_err(|_| MvccError::CorruptedState)?;

            if curr_user_key == user_key {
                if commit_ts <= read_ts {
                    let write_rec =
                        WriteRecord::from_bytes(&write_bytes).ok_or(MvccError::CorruptedState)?;

                    match write_rec.op {
                        OpType::Put => {
                            let data_key = KeyEncoder::encode(user_key, write_rec.start_ts);
                            let value =
                                default_p.get(data_key)?.ok_or(MvccError::CorruptedState)?;
                            return Ok(Some(value.to_vec()));
                        }
                        OpType::Delete => return Ok(None),
                        OpType::Rollback => continue,
                    }
                }
            } else {
                // Iterated past target user_key
                break;
            }
        }

        Ok(None)
    }
}

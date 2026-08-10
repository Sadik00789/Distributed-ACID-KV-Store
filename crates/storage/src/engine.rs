use crate::config::StorageConfig;
use crate::mvcc::{KeyEncoder, WriteRecord};
use fjall::{Batch, Config, Keyspace, PartitionHandle};
use std::path::Path;
use thiserror::Error;
use tracing::info;

pub const PARTITION_DEFAULT: &str = "default";
pub const PARTITION_LOCK: &str = "lock";
pub const PARTITION_WRITE: &str = "write";

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Fjall Storage Engine Error: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("Partition '{0}' not found in keyspace")]
    MissingPartition(String),
}

#[derive(Clone)]
pub struct StorageEngine {
    keyspace: Keyspace,
    p_default: PartitionHandle,
    p_lock: PartitionHandle,
    p_write: PartitionHandle,
}

impl StorageEngine {
    /// Opens or creates a Fjall keyspace at path with required MVCC partitions and custom config.
    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        config: StorageConfig,
    ) -> Result<Self, StorageError> {
        let keyspace_cfg = Config::new(path.as_ref())
            .flush_workers(config.flush_workers)
            .compaction_workers(config.compaction_workers)
            .max_write_buffer_size((config.max_memtable_size_bytes as u64) * 4)
            .max_journaling_size(((config.max_memtable_size_bytes as u64) * 8).max(24 * 1024 * 1024))
            .fsync_ms(config.async_journal_flush_ms.map(|ms| ms as u16));
        let keyspace = keyspace_cfg.open()?;

        let partition_opts = fjall::PartitionCreateOptions::default()
            .max_memtable_size(config.max_memtable_size_bytes);

        let p_default = keyspace.open_partition(PARTITION_DEFAULT, partition_opts.clone())?;
        let p_lock = keyspace.open_partition(PARTITION_LOCK, partition_opts.clone())?;
        let p_write = keyspace.open_partition(PARTITION_WRITE, partition_opts)?;

        info!(path = %path.as_ref().display(), "Opened Fjall StorageEngine instance");

        Ok(Self {
            keyspace,
            p_default,
            p_lock,
            p_write,
        })
    }

    /// Opens or creates a Fjall keyspace at path with default configuration.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        Self::open_with_config(path, StorageConfig::default())
    }

    pub fn default_partition(&self) -> &PartitionHandle {
        &self.p_default
    }

    pub fn lock_partition(&self) -> &PartitionHandle {
        &self.p_lock
    }

    pub fn write_partition(&self) -> &PartitionHandle {
        &self.p_write
    }

    pub fn batch(&self) -> Batch {
        self.keyspace.batch()
    }

    pub fn commit_batch(&self, batch: Batch) -> Result<(), StorageError> {
        batch.commit()?;
        Ok(())
    }

    /// Background MVCC Garbage Collector: removes write/data versions older than safe_point_ts,
    /// keeping only the newest committed version prior to safe_point_ts per key.
    pub fn gc_keys_older_than(&self, safe_point_ts: u64) -> Result<usize, StorageError> {
        let mut batch = self.batch();
        let mut gc_count = 0;
        let mut last_user_key: Option<Vec<u8>> = None;
        let mut seen_below_safe_point = false;

        for item in self.p_write.iter() {
            let (enc_key, write_bytes) = item?;
            let (user_key, commit_ts) = match KeyEncoder::decode(&enc_key) {
                Ok(res) => res,
                Err(_) => continue,
            };

            if last_user_key.as_deref() != Some(user_key) {
                last_user_key = Some(user_key.to_vec());
                seen_below_safe_point = false;
            }

            if commit_ts < safe_point_ts {
                if !seen_below_safe_point {
                    seen_below_safe_point = true;
                } else {
                    batch.remove(&self.p_write, enc_key.clone());
                    if let Some(record) = WriteRecord::from_bytes(&write_bytes) {
                        let data_key = KeyEncoder::encode(user_key, record.start_ts);
                        batch.remove(&self.p_default, data_key);
                    }
                    gc_count += 1;
                }
            }
        }

        if gc_count > 0 {
            self.commit_batch(batch)?;
        }

        Ok(gc_count)
    }

    /// Scans distinct user keys in range [start_key, end_key).
    pub fn get_keys_in_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        let mut keys = Vec::new();
        let mut last_seen: Option<Vec<u8>> = None;

        for item in self.p_write.iter() {
            let (enc_key, _) = item?;
            let (user_key, _) = match KeyEncoder::decode(&enc_key) {
                Ok(res) => res,
                Err(_) => continue,
            };

            if !start_key.is_empty() && user_key < start_key {
                continue;
            }
            if !end_key.is_empty() && user_key >= end_key {
                continue;
            }

            if last_seen.as_deref() != Some(user_key) {
                last_seen = Some(user_key.to_vec());
                keys.push(user_key.to_vec());
            }
        }

        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// Computes key count and total byte size for keys in [start_key, end_key).
    pub fn calculate_region_stats(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<(u64, u64), StorageError> {
        let mut key_count = 0u64;
        let mut total_bytes = 0u64;
        let mut last_seen: Option<Vec<u8>> = None;

        for item in self.p_write.iter() {
            let (enc_key, val_bytes) = item?;
            let (user_key, _) = match KeyEncoder::decode(&enc_key) {
                Ok(res) => res,
                Err(_) => continue,
            };

            if !start_key.is_empty() && user_key < start_key {
                continue;
            }
            if !end_key.is_empty() && user_key >= end_key {
                continue;
            }

            if last_seen.as_deref() != Some(user_key) {
                last_seen = Some(user_key.to_vec());
                key_count += 1;
            }
            total_bytes += enc_key.len() as u64 + val_bytes.len() as u64;
        }

        Ok((key_count, total_bytes))
    }

    /// Finds the median key K_split in range [start_key, end_key).
    pub fn find_median_key(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let keys = self.get_keys_in_range(start_key, end_key)?;
        if keys.len() < 2 {
            Ok(None)
        } else {
            let mid = keys.len() / 2;
            Ok(Some(keys[mid].clone()))
        }
    }
}

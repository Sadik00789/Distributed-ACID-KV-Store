use parking_lot::RwLock;
use protobuf::Message as PbMessage;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{Error as RaftError, GetEntriesContext, RaftState, Storage, StorageError};
use std::sync::Arc;
use storage::StorageEngine;

#[derive(Clone)]
pub struct RaftStorage {
    engine: StorageEngine,
    region_id: u64,
    core: Arc<RwLock<RaftStorageCore>>,
}

struct RaftStorageCore {
    hard_state: HardState,
    conf_state: ConfState,
    entries: Vec<Entry>,
}

fn hs_key(region_id: u64) -> Vec<u8> {
    format!("r_hs_{}", region_id).into_bytes()
}

fn cs_key(region_id: u64) -> Vec<u8> {
    format!("r_cs_{}", region_id).into_bytes()
}

fn entry_key(region_id: u64, idx: u64) -> Vec<u8> {
    format!("r_entry_{}_{:020}", region_id, idx).into_bytes()
}

fn entry_prefix(region_id: u64) -> Vec<u8> {
    format!("r_entry_{}_", region_id).into_bytes()
}

impl RaftStorage {
    pub fn new(engine: StorageEngine, region_id: u64) -> Self {
        let p_default = engine.default_partition();

        let hard_state = match p_default.get(hs_key(region_id)) {
            Ok(Some(bytes)) => HardState::parse_from_bytes(&bytes).unwrap_or_default(),
            _ => HardState::default(),
        };

        let conf_state = match p_default.get(cs_key(region_id)) {
            Ok(Some(bytes)) => ConfState::parse_from_bytes(&bytes).unwrap_or_default(),
            _ => ConfState::default(),
        };

        let mut entries = Vec::new();
        let prefix = entry_prefix(region_id);

        for item in p_default.range(prefix.clone()..) {
            if let Ok((k, v)) = item {
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Ok(entry) = Entry::parse_from_bytes(&v) {
                    entries.push(entry);
                }
            }
        }

        if entries.is_empty() {
            let mut dummy = Entry::default();
            dummy.set_index(0);
            dummy.set_term(0);
            entries.push(dummy);
        }

        Self {
            engine,
            region_id,
            core: Arc::new(RwLock::new(RaftStorageCore {
                hard_state,
                conf_state,
                entries,
            })),
        }
    }

    pub fn append(&self, entries: &[Entry]) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut core = self.core.write();
        let first_new_idx = entries[0].get_index();
        let base_idx = core.entries[0].get_index();

        if first_new_idx < base_idx {
            return Err(StorageError::Compacted);
        }

        let diff = (first_new_idx - base_idx) as usize;
        if core.entries.len() > diff {
            core.entries.truncate(diff);
        }

        core.entries.extend_from_slice(entries);

        let mut batch = self.engine.batch();
        let p_default = self.engine.default_partition();
        for entry in entries {
            if let Ok(bytes) = entry.write_to_bytes() {
                batch.insert(p_default, entry_key(self.region_id, entry.get_index()), bytes);
            }
        }
        let _ = self.engine.commit_batch(batch);

        Ok(())
    }

    pub fn set_hard_state(&self, hs: HardState) {
        let mut core = self.core.write();
        core.hard_state = hs.clone();

        if let Ok(bytes) = hs.write_to_bytes() {
            let mut batch = self.engine.batch();
            batch.insert(self.engine.default_partition(), hs_key(self.region_id), bytes);
            let _ = self.engine.commit_batch(batch);
        }
    }

    pub fn set_conf_state(&self, cs: ConfState) {
        let mut core = self.core.write();
        core.conf_state = cs.clone();

        if let Ok(bytes) = cs.write_to_bytes() {
            let mut batch = self.engine.batch();
            batch.insert(self.engine.default_partition(), cs_key(self.region_id), bytes);
            let _ = self.engine.commit_batch(batch);
        }
    }
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> Result<RaftState, RaftError> {
        let core = self.core.read();
        Ok(RaftState {
            hard_state: core.hard_state.clone(),
            conf_state: core.conf_state.clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        _max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> Result<Vec<Entry>, RaftError> {
        let core = self.core.read();
        let first_idx = core.entries[0].get_index();

        if low < first_idx {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        let last_idx = core.entries.last().map(|e| e.get_index()).unwrap_or(0);
        if high > last_idx + 1 {
            return Err(RaftError::Store(StorageError::Unavailable));
        }

        let start = (low - first_idx) as usize;
        let end = (high - first_idx) as usize;
        Ok(core.entries[start..end].to_vec())
    }

    fn term(&self, idx: u64) -> Result<u64, RaftError> {
        let core = self.core.read();
        let first_idx = core.entries[0].get_index();

        if idx < first_idx {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        let offset = (idx - first_idx) as usize;
        if offset >= core.entries.len() {
            return Err(RaftError::Store(StorageError::Unavailable));
        }

        Ok(core.entries[offset].get_term())
    }

    fn first_index(&self) -> Result<u64, RaftError> {
        let core = self.core.read();
        Ok(core.entries[0].get_index() + 1)
    }

    fn last_index(&self) -> Result<u64, RaftError> {
        let core = self.core.read();
        Ok(core.entries.last().unwrap().get_index())
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> Result<Snapshot, RaftError> {
        let core = self.core.read();
        let mut snap = Snapshot::default();
        let meta = snap.mut_metadata();
        meta.set_index(core.entries[0].get_index());
        meta.set_term(core.entries[0].get_term());
        *meta.mut_conf_state() = core.conf_state.clone();
        Ok(snap)
    }
}

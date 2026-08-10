use parking_lot::RwLock;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{Error as RaftError, GetEntriesContext, RaftState, Storage, StorageError};
use std::sync::Arc;
use storage::StorageEngine;

pub const PREFIX_RAFT_LOG: &[u8] = b"r_log_";
pub const KEY_HARD_STATE: &[u8] = b"r_hard_state";
pub const KEY_CONF_STATE: &[u8] = b"r_conf_state";

#[derive(Clone)]
pub struct RaftStorage {
    _engine: StorageEngine,
    _region_id: u64,
    core: Arc<RwLock<RaftStorageCore>>,
}

struct RaftStorageCore {
    hard_state: HardState,
    conf_state: ConfState,
    entries: Vec<Entry>,
}

impl RaftStorage {
    pub fn new(engine: StorageEngine, region_id: u64) -> Self {
        let mut entries = Vec::new();
        let mut dummy = Entry::default();
        dummy.set_index(0);
        dummy.set_term(0);
        entries.push(dummy);

        Self {
            _engine: engine,
            _region_id: region_id,
            core: Arc::new(RwLock::new(RaftStorageCore {
                hard_state: HardState::default(),
                conf_state: ConfState::default(),
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
        Ok(())
    }

    pub fn set_hard_state(&self, hs: HardState) {
        let mut core = self.core.write();
        core.hard_state = hs;
    }

    pub fn set_conf_state(&self, cs: ConfState) {
        let mut core = self.core.write();
        core.conf_state = cs;
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

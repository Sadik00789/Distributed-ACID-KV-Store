#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub max_memtable_size_bytes: u32,
    pub flush_workers: usize,
    pub compaction_workers: usize,
    pub async_journal_flush_ms: Option<u64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_memtable_size_bytes: 64 * 1024 * 1024,
            flush_workers: 2,
            compaction_workers: 4,
            async_journal_flush_ms: Some(10),
        }
    }
}

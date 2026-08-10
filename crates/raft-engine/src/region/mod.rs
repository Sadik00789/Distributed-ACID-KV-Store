pub mod router;
pub mod splitter;

use serde::{Deserialize, Serialize};
pub use router::RegionRouter;
pub use splitter::RegionSplitter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Region {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub leader_addr: String,
    pub size_bytes: u64,
}

impl Region {
    pub fn contains_key(&self, key: &[u8]) -> bool {
        (self.start_key.is_empty() || key >= self.start_key.as_slice())
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }
}

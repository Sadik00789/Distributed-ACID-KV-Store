use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RouterError {
    #[error("No region found covering key: {0:?}")]
    RegionNotFound(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub peers: Vec<u64>,
}

#[derive(Clone, Default)]
pub struct RegionRouter {
    // Maps start_key -> Region
    regions: Arc<RwLock<BTreeMap<Vec<u8>, Region>>>,
}

impl RegionRouter {
    pub fn new() -> Self {
        Self {
            regions: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn insert_region(&self, region: Region) {
        let mut guard = self.regions.write();
        guard.insert(region.start_key.clone(), region);
    }

    /// Finds the Region covering the specified key.
    pub fn route_key(&self, key: &[u8]) -> Result<Region, RouterError> {
        let guard = self.regions.read();

        // Find region where start_key <= key
        for region in guard.values().rev() {
            if key >= region.start_key.as_slice() {
                if region.end_key.is_empty() || key < region.end_key.as_slice() {
                    return Ok(region.clone());
                }
            }
        }

        Err(RouterError::RegionNotFound(key.to_vec()))
    }
}

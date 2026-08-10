use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum RouterError {
    #[error("No region found covering key: {0:?}")]
    RegionNotFound(Vec<u8>),
    #[error("Region {0} not found")]
    RegionIdNotFound(u64),
    #[error("Region epoch mismatch for region {0}")]
    RegionEpochNotMatch(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RegionEpoch {
    pub conf_ver: u64,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Region {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub peers: Vec<u64>,
    pub epoch: RegionEpoch,
}

impl Region {
    pub fn new(id: u64, start_key: Vec<u8>, end_key: Vec<u8>, peers: Vec<u64>) -> Self {
        Self {
            id,
            start_key,
            end_key,
            peers,
            epoch: RegionEpoch {
                conf_ver: 1,
                version: 1,
            },
        }
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        (self.start_key.is_empty() || key >= self.start_key.as_slice())
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }
}

#[derive(Clone, Default)]
pub struct RegionRouter {
    // Maps region_id -> Region
    regions_by_id: Arc<RwLock<BTreeMap<u64, Region>>>,
    // Maps start_key -> Region
    regions_by_key: Arc<RwLock<BTreeMap<Vec<u8>, Region>>>,
}

impl RegionRouter {
    pub fn new() -> Self {
        Self {
            regions_by_id: Arc::new(RwLock::new(BTreeMap::new())),
            regions_by_key: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn insert_region(&self, region: Region) {
        let mut by_id = self.regions_by_id.write();
        let mut by_key = self.regions_by_key.write();
        by_id.insert(region.id, region.clone());
        by_key.insert(region.start_key.clone(), region);
    }

    pub fn get_region(&self, region_id: u64) -> Option<Region> {
        self.regions_by_id.read().get(&region_id).cloned()
    }

    pub fn route_key(&self, key: &[u8]) -> Result<Region, RouterError> {
        let by_key = self.regions_by_key.read();
        for region in by_key.values().rev() {
            if region.contains_key(key) {
                return Ok(region.clone());
            }
        }
        Err(RouterError::RegionNotFound(key.to_vec()))
    }

    pub fn split_region(
        &self,
        region_id: u64,
        split_key: Vec<u8>,
        new_region_id: u64,
    ) -> Result<(Region, Region), RouterError> {
        let mut by_id = self.regions_by_id.write();
        let mut by_key = self.regions_by_key.write();

        let old_region = by_id
            .get(&region_id)
            .cloned()
            .ok_or(RouterError::RegionIdNotFound(region_id))?;

        if !old_region.contains_key(&split_key) || split_key == old_region.start_key {
            return Err(RouterError::RegionNotFound(split_key));
        }

        by_key.remove(&old_region.start_key);

        let left_region = Region {
            id: old_region.id,
            start_key: old_region.start_key.clone(),
            end_key: split_key.clone(),
            peers: old_region.peers.clone(),
            epoch: RegionEpoch {
                conf_ver: old_region.epoch.conf_ver,
                version: old_region.epoch.version + 1,
            },
        };

        let right_region = Region {
            id: new_region_id,
            start_key: split_key,
            end_key: old_region.end_key.clone(),
            peers: old_region.peers.clone(),
            epoch: RegionEpoch {
                conf_ver: old_region.epoch.conf_ver,
                version: 1,
            },
        };

        by_id.insert(left_region.id, left_region.clone());
        by_id.insert(right_region.id, right_region.clone());
        by_key.insert(left_region.start_key.clone(), left_region.clone());
        by_key.insert(right_region.start_key.clone(), right_region.clone());

        Ok((left_region, right_region))
    }
}

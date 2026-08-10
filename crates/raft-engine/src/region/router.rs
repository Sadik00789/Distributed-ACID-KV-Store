use crate::region::Region;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct RegionRouter {
    // Key range mapping: EndKey -> Region
    regions: Arc<RwLock<BTreeMap<Vec<u8>, Region>>>,
}

impl RegionRouter {
    pub fn new() -> Self {
        Self {
            regions: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn insert_region(&self, region: Region) {
        let mut regions = self.regions.write();
        regions.insert(region.end_key.clone(), region);
    }

    pub fn route(&self, key: &[u8]) -> Option<Region> {
        let regions = self.regions.read();
        for region in regions.values() {
            if region.contains_key(key) {
                return Some(region.clone());
            }
        }
        None
    }
}

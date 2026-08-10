use crate::region::Region;
use tracing::info;

pub const DEFAULT_SPLIT_SIZE_BYTES: u64 = 64 * 1024 * 1024; // 64 MB

pub struct RegionSplitter {
    split_threshold_bytes: u64,
}

impl Default for RegionSplitter {
    fn default() -> Self {
        Self {
            split_threshold_bytes: DEFAULT_SPLIT_SIZE_BYTES,
        }
    }
}

impl RegionSplitter {
    pub fn new(threshold_bytes: u64) -> Self {
        Self {
            split_threshold_bytes: threshold_bytes,
        }
    }

    pub fn should_split(&self, region: &Region) -> bool {
        region.size_bytes >= self.split_threshold_bytes
    }

    pub fn split_region(&self, region: &Region, split_key: Vec<u8>, new_region_id: u64) -> (Region, Region) {
        info!(
            "Splitting Region {} at key {:?} into new Region {}",
            region.id, split_key, new_region_id
        );

        let half_size = region.size_bytes / 2;

        let left_region = Region {
            id: region.id,
            start_key: region.start_key.clone(),
            end_key: split_key.clone(),
            leader_addr: region.leader_addr.clone(),
            size_bytes: half_size,
        };

        let right_region = Region {
            id: new_region_id,
            start_key: split_key,
            end_key: region.end_key.clone(),
            leader_addr: region.leader_addr.clone(),
            size_bytes: half_size,
        };

        (left_region, right_region)
    }
}

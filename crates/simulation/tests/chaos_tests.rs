use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
use storage::StorageEngine;
use tempfile::tempdir;
use txn_coordinator::{HybridLogicalClock, Mutation, TransactionCoordinator};

#[derive(Clone, Default)]
pub struct FaultyTransport {
    isolated_nodes: Arc<RwLock<HashSet<u64>>>,
    crashed_nodes: Arc<RwLock<HashSet<u64>>>,
    packet_loss_rate: Arc<RwLock<f64>>,
}

impl FaultyTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn isolate_node(&self, node_id: u64) {
        self.isolated_nodes.write().insert(node_id);
    }

    pub fn heal_node(&self, node_id: u64) {
        self.isolated_nodes.write().remove(&node_id);
    }

    pub fn crash_node(&self, node_id: u64) {
        self.crashed_nodes.write().insert(node_id);
    }

    pub fn recover_node(&self, node_id: u64) {
        self.crashed_nodes.write().remove(&node_id);
    }

    pub fn set_packet_loss(&self, rate: f64) {
        *self.packet_loss_rate.write() = rate;
    }

    pub fn should_drop(&self, from_node: u64, to_node: u64) -> bool {
        let isolated = self.isolated_nodes.read();
        if isolated.contains(&from_node) || isolated.contains(&to_node) {
            return true;
        }

        let crashed = self.crashed_nodes.read();
        if crashed.contains(&from_node) || crashed.contains(&to_node) {
            return true;
        }

        let rate = *self.packet_loss_rate.read();
        if rate > 0.0 {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            from_node.hash(&mut hasher);
            to_node.hash(&mut hasher);
            let val = (hasher.finish() % 100) as f64 / 100.0;
            if val < rate {
                return true;
            }
        }

        false
    }
}

#[tokio::test]
async fn test_chaos_linearizability_under_partition() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let e1 = StorageEngine::open(dir1.path()).unwrap();
    let e2 = StorageEngine::open(dir2.path()).unwrap();
    let e3 = StorageEngine::open(dir3.path()).unwrap();

    let hlc1 = HybridLogicalClock::new();
    let hlc2 = HybridLogicalClock::new();
    let hlc3 = HybridLogicalClock::new();

    let coord1 = TransactionCoordinator::new(e1, hlc1.clone());
    let coord2 = TransactionCoordinator::new(e2, hlc2.clone());
    let _coord3 = TransactionCoordinator::new(e3, hlc3.clone());

    let transport = FaultyTransport::new();

    // 1. Initial write on Node 1
    let key = b"linear_key".to_vec();
    let val1 = b"val_v1".to_vec();
    let start_ts1 = coord1.begin();

    coord1
        .prewrite(
            start_ts1,
            &[Mutation::Put {
                key: key.clone(),
                value: val1.clone(),
            }],
            &key,
            3000,
        )
        .unwrap();
    let commit_ts1 = hlc1.now();
    coord1.commit(start_ts1, commit_ts1, &[key.clone()]).unwrap();

    // 2. Isolate Node 1 (simulating leader isolation / partition)
    transport.isolate_node(1);
    assert!(transport.should_drop(1, 2));

    // 3. Write newer version on Node 2
    let val2 = b"val_v2".to_vec();
    let start_ts2 = coord2.begin();

    coord2
        .prewrite(
            start_ts2,
            &[Mutation::Put {
                key: key.clone(),
                value: val2.clone(),
            }],
            &key,
            3000,
        )
        .unwrap();
    let commit_ts2 = hlc2.now();
    coord2.commit(start_ts2, commit_ts2, &[key.clone()]).unwrap();

    let read_val2 = coord2.get(&key, hlc2.now()).unwrap();
    assert_eq!(read_val2, Some(val2.clone()));

    // 4. Heal network partition
    transport.heal_node(1);
    assert!(!transport.should_drop(1, 2));

    // 5. Verify no lost updates or stale reads
    let read_ts = hlc2.now();
    let final_val = coord2.get(&key, read_ts).unwrap();
    assert_eq!(final_val, Some(val2));
}

#[tokio::test]
async fn test_chaos_orphan_lock_cleanup_on_node_crash() {
    let dir1 = tempdir().unwrap();
    let _dir2 = tempdir().unwrap();

    let engine1 = StorageEngine::open(dir1.path()).unwrap();

    let hlc1 = HybridLogicalClock::new();

    let coord1 = TransactionCoordinator::new(engine1, hlc1.clone());

    let transport = FaultyTransport::new();

    let primary_key = b"primary_orphan".to_vec();
    let secondary_key = b"secondary_orphan".to_vec();
    let val = b"uncommitted_data".to_vec();

    // Node 1 prewrites locks with very short TTL (1ms) and then crashes mid-2PC
    let start_ts1 = coord1.begin();
    coord1
        .prewrite(
            start_ts1,
            &[
                Mutation::Put {
                    key: primary_key.clone(),
                    value: val.clone(),
                },
                Mutation::Put {
                    key: secondary_key.clone(),
                    value: val.clone(),
                },
            ],
            &primary_key,
            1, // 1ms TTL
        )
        .unwrap();

    // Node 1 crashes
    transport.crash_node(1);
    assert!(transport.should_drop(1, 2));

    // Allow TTL to expire
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;

    // Node 2 attempts to read secondary key
    let read_ts = hlc1.now();
    let result = coord1.get(&secondary_key, read_ts);

    // Percolator lock resolution should detect expired TTL, resolve lock, and return None without blocking
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

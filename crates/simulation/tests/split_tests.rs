use raft_engine::{Region, RegionRouter};
use storage::StorageEngine;
use tempfile::tempdir;

#[test]
fn test_region_range() {
    let region = Region::new(1, b"a".to_vec(), b"z".to_vec(), vec![1, 2, 3]);

    assert_eq!(region.id, 1);
    assert_eq!(region.peers.len(), 3);
    assert!(region.contains_key(b"m"));
    assert!(!region.contains_key(b"0"));
}

#[test]
fn test_region_router_split() {
    let router = RegionRouter::new();
    let initial_region = Region::new(1, vec![], vec![], vec![1]);
    router.insert_region(initial_region);

    let (left, right) = router.split_region(1, b"key_500".to_vec(), 2).unwrap();

    assert_eq!(left.id, 1);
    assert_eq!(left.start_key, Vec::<u8>::new());
    assert_eq!(left.end_key, b"key_500");
    assert_eq!(left.epoch.version, 2);

    assert_eq!(right.id, 2);
    assert_eq!(right.start_key, b"key_500");
    assert_eq!(right.end_key, Vec::<u8>::new());
    assert_eq!(right.epoch.version, 1);

    let routed_left = router.route_key(b"key_100").unwrap();
    assert_eq!(routed_left.id, 1);

    let routed_right = router.route_key(b"key_700").unwrap();
    assert_eq!(routed_right.id, 2);
}

#[test]
fn test_storage_engine_find_median_key() {
    let dir = tempdir().unwrap();
    let engine = StorageEngine::open(dir.path()).unwrap();

    let mut batch = engine.batch();
    let default_p = engine.default_partition();
    let write_p = engine.write_partition();

    for i in 0..10 {
        let k = format!("k_{:02}", i).into_bytes();
        let enc_w = storage::KeyEncoder::encode(&k, 100);
        let rec = storage::WriteRecord::new(100, storage::OpType::Put);
        batch.insert(write_p, enc_w, rec.to_bytes());

        let enc_d = storage::KeyEncoder::encode(&k, 100);
        batch.insert(default_p, enc_d, b"val".to_vec());
    }
    let _ = engine.commit_batch(batch);

    let median = engine.find_median_key(b"", b"").unwrap();
    assert!(median.is_some());
    assert_eq!(median.unwrap(), b"k_05");
}

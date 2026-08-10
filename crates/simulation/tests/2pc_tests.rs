mod common;

use common::harness::TestHarness;
use txn_coordinator::Mutation;

#[tokio::test]
async fn test_2pc_prewrite_commit_flow() {
    let harness = TestHarness::new();
    let start_ts = harness.coordinator.begin();

    let key = b"sim_key".to_vec();
    let val = b"sim_val".to_vec();

    let mutations = vec![Mutation::Put {
        key: key.clone(),
        value: val.clone(),
    }];

    // 1. Prewrite
    harness
        .coordinator
        .prewrite(start_ts, &mutations, &key, 3000)
        .unwrap();

    // 2. Commit
    let commit_ts = harness.hlc.now();
    harness
        .coordinator
        .commit(start_ts, commit_ts, &[key.clone()])
        .unwrap();

    // 3. Read back
    let read_ts = harness.hlc.now();
    let result = harness.coordinator.get(&key, read_ts).unwrap();
    assert_eq!(result, Some(val));
}

#[tokio::test]
async fn test_2pc_lock_resolution_on_expired_ttl() {
    let harness = TestHarness::new();
    let start_ts1 = harness.coordinator.begin();

    let primary_key = b"primary_key".to_vec();
    let secondary_key = b"secondary_key".to_vec();
    let val = b"val1".to_vec();

    let mutations = vec![
        Mutation::Put {
            key: primary_key.clone(),
            value: val.clone(),
        },
        Mutation::Put {
            key: secondary_key.clone(),
            value: val.clone(),
        },
    ];

    // Prewrite primary and secondary locks with 1ms TTL
    harness
        .coordinator
        .prewrite(start_ts1, &mutations, &primary_key, 1)
        .unwrap();

    // Sleep to allow TTL (1ms) to expire
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;

    // Read secondary key from a second transaction
    let start_ts2 = harness.coordinator.begin();
    let res = harness.coordinator.get(&secondary_key, start_ts2);

    assert!(res.is_ok());
    assert_eq!(res.unwrap(), None);
}

#[tokio::test]
async fn test_mvcc_range_scan() {
    let harness = TestHarness::new();

    // Insert 5 keys out of order to verify lexicographical sorting
    let keys = vec![
        b"key_3".to_vec(),
        b"key_1".to_vec(),
        b"key_5".to_vec(),
        b"key_2".to_vec(),
        b"key_4".to_vec(),
    ];

    for (i, key) in keys.into_iter().enumerate() {
        let start_ts = harness.coordinator.begin();
        let val = format!("val_{}", i + 1).into_bytes();
        let mutations = vec![Mutation::Put {
            key: key.clone(),
            value: val,
        }];
        harness
            .coordinator
            .prewrite(start_ts, &mutations, &key, 3000)
            .unwrap();
        let commit_ts = harness.hlc.now();
        harness
            .coordinator
            .commit(start_ts, commit_ts, &[key])
            .unwrap();
    }

    let read_ts = harness.hlc.now();
    let results = harness
        .coordinator
        .scan(b"key_1", b"key_6", 10, read_ts)
        .unwrap();

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0, b"key_1");
    assert_eq!(results[1].0, b"key_2");
    assert_eq!(results[2].0, b"key_3");
    assert_eq!(results[3].0, b"key_4");
    assert_eq!(results[4].0, b"key_5");
}

#[tokio::test]
async fn test_mvcc_gc_keys_older_than() {
    let harness = TestHarness::new();
    let key = b"gc_key".to_vec();

    // Version 1
    let t1 = harness.coordinator.begin();
    harness
        .coordinator
        .prewrite(
            t1,
            &[Mutation::Put {
                key: key.clone(),
                value: b"v1".to_vec(),
            }],
            &key,
            3000,
        )
        .unwrap();
    let c1 = harness.hlc.now();
    harness.coordinator.commit(t1, c1, &[key.clone()]).unwrap();

    // Version 2
    let t2 = harness.coordinator.begin();
    harness
        .coordinator
        .prewrite(
            t2,
            &[Mutation::Put {
                key: key.clone(),
                value: b"v2".to_vec(),
            }],
            &key,
            3000,
        )
        .unwrap();
    let c2 = harness.hlc.now();
    harness.coordinator.commit(t2, c2, &[key.clone()]).unwrap();

    // Version 3
    let t3 = harness.coordinator.begin();
    harness
        .coordinator
        .prewrite(
            t3,
            &[Mutation::Put {
                key: key.clone(),
                value: b"v3".to_vec(),
            }],
            &key,
            3000,
        )
        .unwrap();
    let c3 = harness.hlc.now();
    harness.coordinator.commit(t3, c3, &[key.clone()]).unwrap();

    // GC safe_point at c3: should keep v3 (>= c3) and v2 (newest < c3), removing v1 (< c3).
    let gc_count = harness.engine.gc_keys_older_than(c3).unwrap();
    assert_eq!(gc_count, 1);
}

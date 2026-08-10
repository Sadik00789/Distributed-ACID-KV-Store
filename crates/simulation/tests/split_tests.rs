use raft_engine::Region;

#[test]
fn test_region_range() {
    let region = Region {
        id: 1,
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        peers: vec![1, 2, 3],
    };

    assert_eq!(region.id, 1);
    assert_eq!(region.peers.len(), 3);
}

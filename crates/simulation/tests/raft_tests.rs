mod common;

use common::harness::TestHarness;
use raft::storage::Storage;
use raft_engine::RaftStorage;

#[test]
fn test_raft_storage_initialization() {
    let harness = TestHarness::new();
    let raft_store = RaftStorage::new(harness.engine.clone(), 1);

    assert_eq!(
        raft_store.first_index().unwrap(),
        1,
        "First index should start at 1"
    );
}

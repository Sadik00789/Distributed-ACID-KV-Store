use storage::StorageEngine;
use tempfile::{tempdir, TempDir};
use txn_coordinator::{HybridLogicalClock, TransactionCoordinator};

#[allow(dead_code)]
pub struct TestHarness {
    pub _dir: TempDir,
    pub engine: StorageEngine,
    pub hlc: HybridLogicalClock,
    pub coordinator: TransactionCoordinator,
}

impl TestHarness {
    pub fn new() -> Self {
        let dir = tempdir().expect("Failed to create temp dir");
        let engine = StorageEngine::open(dir.path()).expect("Failed to open storage engine");
        let hlc = HybridLogicalClock::new();
        let coordinator = TransactionCoordinator::new(engine.clone(), hlc.clone());

        Self {
            _dir: dir,
            engine,
            hlc,
            coordinator,
        }
    }
}

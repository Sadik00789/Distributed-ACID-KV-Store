use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp {
    pub physical: u64,
    pub logical: u32,
}

impl Timestamp {
    pub fn encode(&self) -> u64 {
        (self.physical << 16) | (self.logical as u64 & 0xFFFF)
    }

    pub fn decode(ts: u64) -> Self {
        Self {
            physical: ts >> 16,
            logical: (ts & 0xFFFF) as u32,
        }
    }
}

#[derive(Clone)]
pub struct HybridLogicalClock {
    inner: Arc<Mutex<HlcInner>>,
}

struct HlcInner {
    latest_physical: u64,
    logical: u32,
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            inner: Arc::new(Mutex::new(HlcInner {
                latest_physical: now_ms,
                logical: 0,
            })),
        }
    }

    /// Generates a strictly increasing monotonic timestamp.
    pub fn now(&self) -> u64 {
        let physical_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut inner = self.inner.lock();
        if physical_now > inner.latest_physical {
            inner.latest_physical = physical_now;
            inner.logical = 0;
        } else {
            inner.logical += 1;
        }

        Timestamp {
            physical: inner.latest_physical,
            logical: inner.logical,
        }
        .encode()
    }

    /// Updates internal clock state upon receiving a message from a remote node.
    pub fn update(&self, remote_ts: u64) {
        let remote = Timestamp::decode(remote_ts);
        let physical_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut inner = self.inner.lock();
        let max_physical = physical_now.max(inner.latest_physical).max(remote.physical);

        if max_physical == inner.latest_physical && max_physical == remote.physical {
            inner.logical = inner.logical.max(remote.logical) + 1;
        } else if max_physical == inner.latest_physical {
            inner.logical += 1;
        } else if max_physical == remote.physical {
            inner.logical = remote.logical + 1;
        } else {
            inner.logical = 0;
        }

        inner.latest_physical = max_physical;
    }
}

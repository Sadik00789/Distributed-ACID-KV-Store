use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: u64,
    pub heartbeat_tick: usize,
    pub election_tick: usize,
    pub tick_interval: Duration,
    pub max_size_per_msg: u64,
    pub max_inflight_msgs: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            heartbeat_tick: 2,
            election_tick: 10,
            tick_interval: Duration::from_millis(100),
            max_size_per_msg: 1024 * 1024, // 1MB
            max_inflight_msgs: 256,
        }
    }
}

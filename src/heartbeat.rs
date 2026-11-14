/// Coordinator heartbeat and lease expiration monitor.
pub struct LeaseMonitor {
    lease_duration_ms: u64,
    last_heartbeat_ms: u64,
}

impl LeaseMonitor {
    pub fn new(lease_duration_ms: u64, initial_now_ms: u64) -> Self {
        Self {
            lease_duration_ms,
            last_heartbeat_ms: initial_now_ms,
        }
    }

    pub fn record_heartbeat(&mut self, now_ms: u64) {
        if now_ms >= self.last_heartbeat_ms {
            self.last_heartbeat_ms = now_ms;
        }
    }

    pub fn is_lease_valid(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.last_heartbeat_ms) < self.lease_duration_ms
    }

    pub fn time_remaining_ms(&self, now_ms: u64) -> u64 {
        let elapsed = now_ms.saturating_sub(self.last_heartbeat_ms);
        self.lease_duration_ms.saturating_sub(elapsed)
    }
}

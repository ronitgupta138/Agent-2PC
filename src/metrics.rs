/// In-memory accumulator for Two-Phase Commit transaction phase latency.
#[derive(Default)]
pub struct PhaseLatencyCollector {
    prepare_latencies: Vec<u64>,
    commit_latencies: Vec<u64>,
}

impl PhaseLatencyCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_prepare(&mut self, latency_ms: u64) {
        self.prepare_latencies.push(latency_ms);
    }

    pub fn record_commit(&mut self, latency_ms: u64) {
        self.commit_latencies.push(latency_ms);
    }

    pub fn total_transactions(&self) -> usize {
        self.commit_latencies.len()
    }

    pub fn avg_prepare_ms(&self) -> f64 {
        if self.prepare_latencies.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.prepare_latencies.iter().sum();
        sum as f64 / self.prepare_latencies.len() as f64
    }

    pub fn avg_commit_ms(&self) -> f64 {
        if self.commit_latencies.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.commit_latencies.iter().sum();
        sum as f64 / self.commit_latencies.len() as f64
    }
}

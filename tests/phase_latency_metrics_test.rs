use agent_2pc::metrics::PhaseLatencyCollector;

#[test]
fn test_phase_latency_averages() {
    let mut collector = PhaseLatencyCollector::new();
    collector.record_prepare(10);
    collector.record_prepare(20);
    collector.record_commit(5);
    collector.record_commit(15);

    assert_eq!(collector.total_transactions(), 2);
    assert_eq!(collector.avg_prepare_ms(), 15.0);
    assert_eq!(collector.avg_commit_ms(), 10.0);
}

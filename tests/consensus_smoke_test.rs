use agent_2pc::heartbeat::LeaseMonitor;
use agent_2pc::checksum::compute_frame_crc32;
use agent_2pc::fencing::FencingTokenValidator;

#[test]
fn test_consensus_pipeline_smoke() {
    let monitor = LeaseMonitor::new(500, 1000);
    assert!(monitor.is_lease_valid(1200));

    let mut fencing = FencingTokenValidator::new(1);
    assert!(fencing.validate_and_advance(2));

    let frame = b"PREPARE:2";
    let crc = compute_frame_crc32(frame);
    assert!(crc > 0);
}

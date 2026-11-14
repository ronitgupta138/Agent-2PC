use agent_2pc::checksum::{compute_frame_crc32, verify_frame_crc32};

#[test]
fn test_crc32_computation_and_tamper_detection() {
    let payload = b"GLOBAL_COMMIT:tx_1001:participant_shard_3";
    let crc = compute_frame_crc32(payload);
    assert_ne!(crc, 0);
    assert!(verify_frame_crc32(payload, crc));

    // Tampered payload
    let mut tampered = payload.to_vec();
    tampered[0] = b'X';
    assert!(!verify_frame_crc32(&tampered, crc));
}

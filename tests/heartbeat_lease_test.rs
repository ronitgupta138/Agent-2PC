use agent_2pc::heartbeat::LeaseMonitor;

#[test]
fn test_lease_validity_and_expiration() {
    let mut monitor = LeaseMonitor::new(1000, 10000);
    assert!(monitor.is_lease_valid(10500));
    assert_eq!(monitor.time_remaining_ms(10500), 500);

    // Exactly at expiry
    assert!(!monitor.is_lease_valid(11000));
    assert_eq!(monitor.time_remaining_ms(11000), 0);

    // Renew heartbeat
    monitor.record_heartbeat(11050);
    assert!(monitor.is_lease_valid(11100));
    assert_eq!(monitor.time_remaining_ms(11100), 950);
}

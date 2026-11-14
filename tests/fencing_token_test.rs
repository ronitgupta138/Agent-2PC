use agent_2pc::fencing::FencingTokenValidator;

#[test]
fn test_fencing_token_monotonic_advance() {
    let mut validator = FencingTokenValidator::new(10);
    assert_eq!(validator.current_epoch(), 10);

    // Stale or duplicate tokens rejected
    assert!(!validator.validate_and_advance(9));
    assert!(!validator.validate_and_advance(10));
    assert_eq!(validator.current_epoch(), 10);

    // Higher epoch accepted and advanced
    assert!(validator.validate_and_advance(11));
    assert_eq!(validator.current_epoch(), 11);
    assert!(validator.validate_and_advance(15));
    assert_eq!(validator.current_epoch(), 15);
}

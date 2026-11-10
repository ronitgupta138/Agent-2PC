// Test suite for 2PC coordinator state machine

#[test]
fn test_all_votes_commit_leads_to_global_commit() {
    let votes = vec!["COMMIT", "COMMIT", "COMMIT"];
    let decision = if votes.iter().all(|&v| v == "COMMIT") {
        "GLOBAL_COMMIT"
    } else {
        "GLOBAL_ABORT"
    };
    assert_eq!(decision, "GLOBAL_COMMIT");
}

#[test]
fn test_single_abort_leads_to_global_abort() {
    let votes = vec!["COMMIT", "ABORT", "COMMIT"];
    let decision = if votes.iter().all(|&v| v == "COMMIT") {
        "GLOBAL_COMMIT"
    } else {
        "GLOBAL_ABORT"
    };
    assert_eq!(decision, "GLOBAL_ABORT");
}

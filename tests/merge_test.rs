use agent_2pc::merge::{MergeResult, ThreeWayMerger};

#[test]
fn test_three_way_merge_non_overlapping_clean() {
    let base = "\
// Module Header
fn alpha() {
    println!(\"alpha v1\");
}

fn beta() {
    println!(\"beta v1\");
}

fn gamma() {
    println!(\"gamma v1\");
}
";

    // Agent A modifies alpha
    let ours = "\
// Module Header
fn alpha() {
    println!(\"alpha updated by Agent A\");
}

fn beta() {
    println!(\"beta v1\");
}

fn gamma() {
    println!(\"gamma v1\");
}
";

    // Agent B modifies gamma
    let theirs = "\
// Module Header
fn alpha() {
    println!(\"alpha v1\");
}

fn beta() {
    println!(\"beta v1\");
}

fn gamma() {
    println!(\"gamma updated by Agent B\");
}
";

    let result = ThreeWayMerger::merge(base, ours, theirs);
    assert!(matches!(result, MergeResult::Clean(_)));

    if let MergeResult::Clean(merged) = result {
        assert!(merged.contains("alpha updated by Agent A"));
        assert!(merged.contains("gamma updated by Agent B"));
        assert!(merged.contains("beta v1"));
    }
}

#[test]
fn test_three_way_merge_overlapping_conflict() {
    let base = "\
fn execute() {
    let x = 10;
}
";

    let ours = "\
fn execute() {
    let x = 20; // Changed by Agent A
}
";

    let theirs = "\
fn execute() {
    let x = 30; // Changed by Agent B
}
";

    let result = ThreeWayMerger::merge(base, ours, theirs);
    assert!(matches!(result, MergeResult::Conflict { .. }));

    if let MergeResult::Conflict { merged_with_markers, conflict_count } = result {
        assert_eq!(conflict_count, 1);
        assert!(merged_with_markers.contains("<<<<<<< OURS"));
        assert!(merged_with_markers.contains(">>>>>>> THEIRS"));
    }
}

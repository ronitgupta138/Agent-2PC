//! Integration tests for Transparent Command Execution Staging Engine.

use agent_2pc::coordinator::Coordinator;
use agent_2pc::domain::TxState;
use agent_2pc::exec::ExecRunner;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_exec_runner_autostages_new_and_modified_files() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let tid = coord.begin(Some(10000)).unwrap();

    // Create an existing file
    let existing_file = tmp.path().join("existing.txt");
    fs::write(&existing_file, "original content\n").unwrap();

    // Run shell command creating a new file and modifying the existing file
    let cmd = vec![
        "sh".to_string(),
        "-c".to_string(),
        "echo 'created by script' > created.txt && echo 'appended line' >> existing.txt".to_string(),
    ];

    let report = ExecRunner::execute_and_stage(&mut coord, &tid, tmp.path(), &cmd).unwrap();

    assert_eq!(report.exit_code, 0);
    assert_eq!(report.staged_files.len(), 2);

    let tx = coord.get_status(&tid).unwrap();
    assert_eq!(tx.participants.len(), 2);
    assert_eq!(tx.receipts.len(), 2);
    assert_eq!(tx.state, TxState::Prepared);

    // Prior to commit, existing file was preserved and created file is staged
    assert_eq!(fs::read_to_string(&existing_file).unwrap(), "original content\n");
    assert!(!tmp.path().join("created.txt").exists());

    // Commit transaction
    let mut adapters = agent_2pc::adapters::reconstruct_adapters(&tx.participants);
    coord.commit(&tid, &mut adapters[..]).unwrap();

    // After commit, both mutations materialize
    assert!(tmp.path().join("created.txt").exists());
    assert_eq!(
        fs::read_to_string(tmp.path().join("created.txt")).unwrap().trim(),
        "created by script"
    );
    assert!(fs::read_to_string(&existing_file).unwrap().contains("appended line"));
}

#[test]
fn test_exec_runner_failed_command_does_not_stage_and_reverts() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let tid = coord.begin(Some(10000)).unwrap();

    let existing_file = tmp.path().join("important.txt");
    fs::write(&existing_file, "do not touch").unwrap();

    // Run command that mutates but exits with non-zero code
    let cmd = vec![
        "sh".to_string(),
        "-c".to_string(),
        "echo 'corrupted' > important.txt && exit 42".to_string(),
    ];

    let report = ExecRunner::execute_and_stage(&mut coord, &tid, tmp.path(), &cmd).unwrap();

    assert_eq!(report.exit_code, 42);
    assert!(report.staged_files.is_empty());

    // Workspace was reverted immediately
    assert_eq!(fs::read_to_string(&existing_file).unwrap(), "do not touch");

    let tx = coord.get_status(&tid).unwrap();
    assert_eq!(tx.participants.len(), 0);
}

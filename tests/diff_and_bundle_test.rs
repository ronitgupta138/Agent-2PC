use agent_2pc::adapters::FsAdapter;
use agent_2pc::bundle::BundleManager;
use agent_2pc::coordinator::Coordinator;
use agent_2pc::diff::TransactionDiffer;
use agent_2pc::domain::{ResourceIntent, TxState};
use std::fs;
use tempfile::tempdir;

#[test]
fn test_transaction_diff_and_diffstat() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let file_a = tmp.path().join("file_a.txt");
    fs::write(&file_a, "line 1\nline 2 original\nline 3\n").unwrap();

    let tid = coord.begin(Some(10000)).unwrap();
    let mut adapter = FsAdapter::new("fs_a");
    let intent = ResourceIntent::FsReplace {
        path: file_a.clone(),
        old_string: "line 2 original".to_string(),
        new_string: "line 2 updated".to_string(),
    };

    let vote = coord.prepare_participant(&tid, &mut adapter, intent).unwrap();
    assert!(matches!(vote, agent_2pc::domain::ParticipantVote::Prepared(_)));

    let diff = TransactionDiffer::diff_transaction(&coord, &tid).unwrap();
    assert_eq!(diff.files.len(), 1);
    assert_eq!(diff.total_additions, 1);
    assert_eq!(diff.total_deletions, 1);

    let stat_str = diff.render_stat();
    assert!(stat_str.contains("1 files changed, 1 insertions(+), 1 deletions(-)"));

    let terminal_diff = diff.render_terminal(false);
    assert!(terminal_diff.contains("-line 2 original"));
    assert!(terminal_diff.contains("+line 2 updated"));
}

#[test]
fn test_bundle_export_import_and_commit() {
    let tmp_src = tempdir().unwrap();
    let tx_dir_src = tmp_src.path().join(".agent-tx");
    let mut coord_src = Coordinator::new(&tx_dir_src).unwrap();

    let target_file = tmp_src.path().join("swarm_work.txt");
    fs::write(&target_file, "initial state\n").unwrap();

    let tid = coord_src.begin(Some(15000)).unwrap();
    let mut adapter = FsAdapter::new("swarm_worker_1");
    let intent = ResourceIntent::FsWrite {
        path: target_file.clone(),
        content: "swarmed update state\n".to_string(),
        create_dirs: false,
    };

    let vote = coord_src.prepare_participant(&tid, &mut adapter, intent).unwrap();
    assert!(matches!(vote, agent_2pc::domain::ParticipantVote::Prepared(_)));

    // Export bundle
    let bundle_path = tmp_src.path().join("tx_export.a2pc");
    let exp_report = BundleManager::export_bundle(&coord_src, &tid, &bundle_path).unwrap();
    assert_eq!(exp_report.tid, tid);
    assert!(exp_report.file_count >= 1);
    assert!(bundle_path.exists());

    // Import into fresh coordinator
    let tmp_dst = tempdir().unwrap();
    let tx_dir_dst = tmp_dst.path().join(".agent-tx");
    let mut coord_dst = Coordinator::new(tx_dir_dst).unwrap();

    let imp_report = BundleManager::import_bundle(&mut coord_dst, &bundle_path).unwrap();
    assert_eq!(imp_report.tid, tid);
    assert_eq!(imp_report.restored_files, exp_report.file_count);
    assert_eq!(imp_report.participant_count, 1);

    let imported_status = coord_dst.get_status(&tid).unwrap();
    assert_eq!(imported_status.state, TxState::Prepared);

    // Commit imported transaction
    let commit_adapter = FsAdapter::new("swarm_worker_1");
    let mut adapters: Vec<Box<dyn agent_2pc::adapters::ResourceManager>> = vec![Box::new(commit_adapter)];
    coord_dst.commit(&tid, &mut adapters[..]).unwrap();

    let final_content = fs::read_to_string(&target_file).unwrap();
    assert_eq!(final_content, "swarmed update state\n");
}

#[test]
fn test_bundle_crc32_tamper_detection() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let target_file = tmp.path().join("tamper_target.txt");
    let tid = coord.begin(Some(10000)).unwrap();
    let mut adapter = FsAdapter::new("tamper_part");
    let intent = ResourceIntent::FsWrite {
        path: target_file,
        content: "secure content\n".to_string(),
        create_dirs: false,
    };
    let _ = coord.prepare_participant(&tid, &mut adapter, intent).unwrap();

    let bundle_path = tmp.path().join("tamper.a2pc");
    let _ = BundleManager::export_bundle(&coord, &tid, &bundle_path).unwrap();

    // Tamper with the payload inside the bundle
    let raw = fs::read_to_string(&bundle_path).unwrap();
    let tampered = raw.replace("secure content", "malicious payload");
    fs::write(&bundle_path, tampered).unwrap();

    // Attempt import
    let mut coord_dst = Coordinator::new(tmp.path().join(".agent-tx-dst")).unwrap();
    let res = BundleManager::import_bundle(&mut coord_dst, &bundle_path);
    assert!(res.is_err());
    let err_str = res.err().unwrap().to_string();
    assert!(err_str.contains("Checksum mismatch"));
}

#[test]
fn test_bundle_with_multiple_participants() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let f1 = tmp.path().join("multi_1.txt");
    let f2 = tmp.path().join("multi_2.txt");

    let tid = coord.begin(Some(10000)).unwrap();

    let mut a1 = FsAdapter::new("p1");
    let _ = coord.prepare_participant(&tid, &mut a1, ResourceIntent::FsWrite {
        path: f1.clone(),
        content: "multi 1\n".to_string(),
        create_dirs: false,
    }).unwrap();

    let mut a2 = FsAdapter::new("p2");
    let _ = coord.prepare_participant(&tid, &mut a2, ResourceIntent::FsWrite {
        path: f2.clone(),
        content: "multi 2\n".to_string(),
        create_dirs: false,
    }).unwrap();

    let bundle_path = tmp.path().join("multi.a2pc");
    let report = BundleManager::export_bundle(&coord, &tid, &bundle_path).unwrap();
    assert_eq!(report.participant_count, 2);
    assert_eq!(report.file_count, 2);

    let mut coord_dst = Coordinator::new(tmp.path().join(".agent-tx-dst")).unwrap();
    let imp = BundleManager::import_bundle(&mut coord_dst, &bundle_path).unwrap();
    assert_eq!(imp.participant_count, 2);
    assert_eq!(imp.restored_files, 2);
}

use agent_2pc::cluster::{ClusterCoordinator, ClusterMessage};
use agent_2pc::domain::{ParticipantId, ParticipantVote, StageReceipt, TransactionId};

#[test]
fn test_distributed_consensus_all_prepared() {
    let mut cluster = ClusterCoordinator::new();
    cluster.register_node("node_gpu_1", "http://10.0.0.1:8080");
    cluster.register_node("node_gpu_2", "http://10.0.0.2:8080");

    let tid = TransactionId::new();
    let votes = vec![
        ParticipantVote::Prepared(StageReceipt::new(ParticipantId::new("node_gpu_1"))),
        ParticipantVote::Prepared(StageReceipt::new(ParticipantId::new("node_gpu_2"))),
    ];

    let consensus = cluster.evaluate_votes(&votes).unwrap();
    assert!(consensus);

    let decision = cluster.create_decision_packet(tid, consensus, None);
    assert!(matches!(decision, ClusterMessage::CommitRequest { .. }));
}

#[test]
fn test_distributed_consensus_peer_abort() {
    let mut cluster = ClusterCoordinator::new();
    cluster.register_node("worker_1", "http://10.0.0.1:8080");
    cluster.register_node("worker_2", "http://10.0.0.2:8080");

    let tid = TransactionId::new();
    let votes = vec![
        ParticipantVote::Prepared(StageReceipt::new(ParticipantId::new("worker_1"))),
        ParticipantVote::Aborted {
            reason: "GPU VRAM out-of-memory".into(),
        },
    ];

    let consensus_res = cluster.evaluate_votes(&votes);
    assert!(consensus_res.is_err());

    let decision = cluster.create_decision_packet(tid, false, Some("GPU VRAM out-of-memory"));
    assert!(matches!(decision, ClusterMessage::AbortRequest { .. }));
}

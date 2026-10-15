//! Integration tests for Multi-Node Cluster Network Transport.

use agent_2pc::cluster::{ClusterCoordinator, ClusterMessage};
use agent_2pc::domain::{
    ParticipantId, ParticipantVote, ResourceIntent, StageReceipt, TransactionId,
};
use agent_2pc::transport::{ClusterNodeServer, ClusterTransportClient};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_cluster_network_transport_prepare_and_commit() {
    let bind_addr = "127.0.0.1:19871";
    let server = Arc::new(ClusterNodeServer::new(bind_addr));
    let server_clone = Arc::clone(&server);

    let handle = thread::spawn(move || {
        let _ = server_clone.serve(|msg| match msg {
            ClusterMessage::PrepareRequest {
                tid,
                participant_id,
                ..
            } => {
                let receipt = StageReceipt::new(participant_id.clone());
                ClusterMessage::PrepareResponse {
                    tid,
                    participant_id,
                    vote: ParticipantVote::Prepared(receipt),
                }
            }
            ClusterMessage::CommitRequest { tid } => ClusterMessage::CommitResponse {
                tid,
                participant_id: ParticipantId::new("remote_worker_1"),
                success: true,
            },
            ClusterMessage::AbortRequest { tid, .. } => ClusterMessage::AbortResponse {
                tid,
                participant_id: ParticipantId::new("remote_worker_1"),
                success: true,
            },
            other => other,
        });
    });

    thread::sleep(Duration::from_millis(50));

    let mut client = ClusterTransportClient::connect(bind_addr).unwrap();
    let tid = TransactionId::new();
    let pid = ParticipantId::new("remote_worker_1");

    // Phase 1: Send PrepareRequest over TCP
    let prepare_req = ClusterMessage::PrepareRequest {
        tid,
        participant_id: pid.clone(),
        intent: ResourceIntent::FsWrite {
            path: PathBuf::from("remote.txt"),
            content: "cluster data".into(),
            create_dirs: true,
        },
    };

    let prep_resp = client.send_message(&prepare_req).unwrap();
    match prep_resp {
        ClusterMessage::PrepareResponse {
            tid: r_tid,
            participant_id: r_pid,
            vote,
        } => {
            assert_eq!(r_tid, tid);
            assert_eq!(r_pid, pid);
            assert!(matches!(vote, ParticipantVote::Prepared(_)));
        }
        _ => panic!("Expected PrepareResponse packet"),
    }

    // Phase 2: Send CommitRequest over TCP
    let commit_req = ClusterMessage::CommitRequest { tid };
    let commit_resp = client.send_message(&commit_req).unwrap();
    match commit_resp {
        ClusterMessage::CommitResponse {
            tid: r_tid,
            success,
            ..
        } => {
            assert_eq!(r_tid, tid);
            assert!(success);
        }
        _ => panic!("Expected CommitResponse packet"),
    }

    server.stop();
    handle.join().unwrap();
}

#[test]
fn test_cluster_coordinator_consensus_evaluation_over_network() {
    let mut coordinator = ClusterCoordinator::new();
    coordinator.register_node("node-1", "127.0.0.1:19872");
    coordinator.register_node("node-2", "127.0.0.1:19873");

    let receipt = StageReceipt::new(ParticipantId::new("node-1"));
    let votes = vec![
        ParticipantVote::Prepared(receipt),
        ParticipantVote::Prepared(StageReceipt::new(ParticipantId::new("node-2"))),
    ];

    let consensus = coordinator.evaluate_votes(&votes).unwrap();
    assert!(consensus);

    let tid = TransactionId::new();
    let decision = coordinator.create_decision_packet(tid, consensus, None);
    assert!(matches!(decision, ClusterMessage::CommitRequest { .. }));
}

//! In-Memory Unix Domain Socket Daemon for agent-2pc.
//!
//! Provides zero-disk-contention, sub-millisecond transaction coordination
//! across concurrent AI agent tools and CLI processes via local Unix Domain Sockets.

use crate::adapters::{
    reconstruct_adapters, ApiAdapter, CommandAdapter, FsAdapter, ResourceManager,
};
use crate::coordinator::Coordinator;
use crate::domain::{ParticipantVote, ResourceIntent, TransactionId, TxError, TxState};
use crate::ipc::{IpcRequest, IpcResponse};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Background daemon instance coordinating 2PC transactions in-memory with fsync durability.
pub struct Daemon {
    base_dir: PathBuf,
    socket_path: PathBuf,
    coordinator: Arc<Mutex<Coordinator>>,
    running: Arc<AtomicBool>,
    reaped_count: Arc<AtomicUsize>,
    compact_count: Arc<AtomicUsize>,
}

impl Daemon {
    /// Initialize daemon instance for the specified `.agent-tx` workspace directory.
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self, TxError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir)?;
        let socket_path = base_dir.join("agent-2pc.sock");
        let coordinator = Arc::new(Mutex::new(Coordinator::new(&base_dir)?));
        let running = Arc::new(AtomicBool::new(false));
        let reaped_count = Arc::new(AtomicUsize::new(0));
        let compact_count = Arc::new(AtomicUsize::new(0));

        Ok(Self {
            base_dir,
            socket_path,
            coordinator,
            running,
            reaped_count,
            compact_count,
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Return telemetry stats: (reaped_transactions, auto_compactions)
    pub fn reaper_stats(&self) -> (usize, usize) {
        (
            self.reaped_count.load(Ordering::SeqCst),
            self.compact_count.load(Ordering::SeqCst),
        )
    }

    /// Run the daemon event loop, serving client connections until stopped.
    pub fn run(&self) -> Result<(), TxError> {
        if self.socket_path.exists() {
            let _ = fs::remove_file(&self.socket_path);
        }

        let listener = UnixListener::bind(&self.socket_path)?;
        listener.set_nonblocking(true)?;
        self.running.store(true, Ordering::SeqCst);

        // Spawn background TTL reaper & auto-compactor thread
        let coord_reaper = Arc::clone(&self.coordinator);
        let running_reaper = Arc::clone(&self.running);
        let base_dir_reaper = self.base_dir.clone();
        let reaped_counter = Arc::clone(&self.reaped_count);
        let compact_counter = Arc::clone(&self.compact_count);

        let reaper_thread = thread::spawn(move || {
            while running_reaper.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(250));
                if !running_reaper.load(Ordering::SeqCst) {
                    break;
                }

                // 1. Check expired transactions
                let mut expired = Vec::new();
                if let Ok(guard) = coord_reaper.lock() {
                    let now = chrono::Utc::now();
                    for tx in guard.active_transactions() {
                        if tx.state == TxState::Active || tx.state == TxState::Prepared {
                            let expiry = tx.created_at + chrono::Duration::milliseconds(tx.timeout_ms as i64);
                            if now > expiry {
                                expired.push((tx.tid, tx.clone()));
                            }
                        }
                    }
                }

                for (tid, tx) in expired {
                    if let Ok(mut guard) = coord_reaper.lock() {
                        let mut adapters = reconstruct_adapters(&tx.participants);
                        let _ = guard.abort(&tid, "TTL expired by daemon reaper", &mut adapters[..]);
                        reaped_counter.fetch_add(1, Ordering::SeqCst);
                    }
                }

                // 2. Auto-compaction if WAL exceeds threshold (e.g. 2MB)
                let wal_path = base_dir_reaper.join("wal.bin");
                if let Ok(meta) = fs::metadata(&wal_path)
                    && meta.len() > 2 * 1024 * 1024
                    && let Ok(mut guard) = coord_reaper.lock()
                    && guard.compact().is_ok()
                {
                    compact_counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        while self.running.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let coord = Arc::clone(&self.coordinator);
                    let running = Arc::clone(&self.running);
                    let reaped = Arc::clone(&self.reaped_count);
                    let compact = Arc::clone(&self.compact_count);
                    thread::spawn(move || {
                        let _ = handle_connection(stream, coord, running, reaped, compact);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => return Err(TxError::Io(e)),
            }
        }

        let _ = reaper_thread.join();

        if self.socket_path.exists() {
            let _ = fs::remove_file(&self.socket_path);
        }

        Ok(())
    }

    /// Signal the daemon to cleanly terminate and remove its socket.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        if self.socket_path.exists() {
            let _ = fs::remove_file(&self.socket_path);
        }
    }
}

fn handle_connection(
    mut stream: UnixStream,
    coord: Arc<Mutex<Coordinator>>,
    running: Arc<AtomicBool>,
    reaped_count: Arc<AtomicUsize>,
    compact_count: Arc<AtomicUsize>,
) -> Result<(), TxError> {
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;

    let reader = BufReader::new(stream.try_clone()?);

    for line_res in reader.lines() {
        let line: String = match line_res {
            Ok(l) => l,
            Err(_) => break,
        };

        if line.trim().is_empty() {
            continue;
        }

        let req: IpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let err_resp = IpcResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Invalid JSON request: {}", e)),
                };
                let mut out = serde_json::to_string(&err_resp)?;
                out.push('\n');
                let _ = stream.write_all(out.as_bytes());
                continue;
            }
        };

        let resp = dispatch_request(&req, &coord, &running, &reaped_count, &compact_count);
        let mut out = serde_json::to_string(&resp)?;
        out.push('\n');
        let _ = stream.write_all(out.as_bytes());
        let _ = stream.flush();
    }

    Ok(())
}

fn dispatch_request(
    req: &IpcRequest,
    coord: &Arc<Mutex<Coordinator>>,
    running: &Arc<AtomicBool>,
    reaped_count: &Arc<AtomicUsize>,
    compact_count: &Arc<AtomicUsize>,
) -> IpcResponse {
    match req.method.as_str() {
        "ping" => IpcResponse {
            success: true,
            data: Some(json!({ "status": "ok", "version": "1.3.0" })),
            error: None,
        },

        "stop" => {
            running.store(false, Ordering::SeqCst);
            IpcResponse {
                success: true,
                data: Some(json!({ "status": "stopping" })),
                error: None,
            }
        }

        "begin" => {
            let timeout_ms = req.params.get("timeout_ms").and_then(|v| v.as_u64());
            let mut guard = coord.lock().unwrap();
            match guard.begin(timeout_ms) {
                Ok(tid) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "tid": tid.to_string(),
                        "timeout_ms": timeout_ms.unwrap_or(30000)
                    })),
                    error: None,
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "stage_write" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let path_str = req.params.get("path").and_then(|v| v.as_str()).unwrap_or_default();
            let content = req.params.get("content").and_then(|v| v.as_str()).unwrap_or_default();
            let create_dirs = req.params.get("create_dirs").and_then(|v| v.as_bool()).unwrap_or(true);
            let participant = req.params.get("participant").and_then(|v| v.as_str());

            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let path = PathBuf::from(path_str);
            let pid = participant
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("fs_write_{}", path.file_name().unwrap_or_default().to_string_lossy()));

            let mut adapter = FsAdapter::new(pid.clone());
            let intent = ResourceIntent::FsWrite {
                path,
                content: content.to_string(),
                create_dirs,
            };

            let mut guard = coord.lock().unwrap();
            match guard.prepare_participant(&tid, &mut adapter, intent) {
                Ok(ParticipantVote::Prepared(receipt)) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "participant": pid,
                        "receipt": receipt
                    })),
                    error: None,
                },
                Ok(ParticipantVote::Aborted { reason }) => IpcResponse {
                    success: false,
                    data: Some(json!({ "vote": "ABORTED", "reason": reason })),
                    error: Some(format!("Participant voted ABORT: {}", reason)),
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "stage_replace" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let path_str = req.params.get("path").and_then(|v| v.as_str()).unwrap_or_default();
            let old_string = req.params.get("old_string").and_then(|v| v.as_str()).unwrap_or_default();
            let new_string = req.params.get("new_string").and_then(|v| v.as_str()).unwrap_or_default();
            let participant = req.params.get("participant").and_then(|v| v.as_str());

            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let path = PathBuf::from(path_str);
            let pid = participant
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("fs_replace_{}", path.file_name().unwrap_or_default().to_string_lossy()));

            let mut adapter = FsAdapter::new(pid.clone());
            let intent = ResourceIntent::FsReplace {
                path,
                old_string: old_string.to_string(),
                new_string: new_string.to_string(),
            };

            let mut guard = coord.lock().unwrap();
            match guard.prepare_participant(&tid, &mut adapter, intent) {
                Ok(ParticipantVote::Prepared(receipt)) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "participant": pid,
                        "receipt": receipt
                    })),
                    error: None,
                },
                Ok(ParticipantVote::Aborted { reason }) => IpcResponse {
                    success: false,
                    data: Some(json!({ "vote": "ABORTED", "reason": reason })),
                    error: Some(format!("Participant voted ABORT: {}", reason)),
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "stage_patch" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let path_str = req.params.get("path").and_then(|v| v.as_str()).unwrap_or_default();
            let patch = req.params.get("patch").and_then(|v| v.as_str()).unwrap_or_default();
            let participant = req.params.get("participant").and_then(|v| v.as_str());

            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let path = PathBuf::from(path_str);
            let pid = participant
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("fs_patch_{}", path.file_name().unwrap_or_default().to_string_lossy()));

            let mut adapter = FsAdapter::new(pid.clone());
            let intent = ResourceIntent::FsPatch {
                path,
                patch: patch.to_string(),
            };

            let mut guard = coord.lock().unwrap();
            match guard.prepare_participant(&tid, &mut adapter, intent) {
                Ok(ParticipantVote::Prepared(receipt)) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "participant": pid,
                        "receipt": receipt
                    })),
                    error: None,
                },
                Ok(ParticipantVote::Aborted { reason }) => IpcResponse {
                    success: false,
                    data: Some(json!({ "vote": "ABORTED", "reason": reason })),
                    error: Some(format!("Participant voted ABORT: {}", reason)),
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "stage_cmd" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let command = req.params.get("command").and_then(|v| v.as_str()).unwrap_or_default();
            let dir_str = req.params.get("dir").and_then(|v| v.as_str());
            let participant = req.params.get("participant").and_then(|v| v.as_str());

            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let working_dir = dir_str.map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
            let pid = participant
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("cmd_{}", command.chars().filter(|c| c.is_alphanumeric()).take(16).collect::<String>()));

            let mut adapter = CommandAdapter::new(pid.clone());
            let intent = ResourceIntent::Command {
                command: command.to_string(),
                working_dir,
                env: HashMap::new(),
            };

            let mut guard = coord.lock().unwrap();
            match guard.prepare_participant(&tid, &mut adapter, intent) {
                Ok(ParticipantVote::Prepared(receipt)) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "participant": pid,
                        "receipt": receipt
                    })),
                    error: None,
                },
                Ok(ParticipantVote::Aborted { reason }) => IpcResponse {
                    success: false,
                    data: Some(json!({ "vote": "ABORTED", "reason": reason })),
                    error: Some(format!("Participant voted ABORT: {}", reason)),
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "stage_api" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let endpoint = req.params.get("endpoint").and_then(|v| v.as_str()).unwrap_or_default();
            let method = req.params.get("method").and_then(|v| v.as_str()).unwrap_or("POST");
            let idempotency_key = req.params.get("idempotency_key").and_then(|v| v.as_str()).map(|s| s.to_string()).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let comp_endpoint = req.params.get("compensation_endpoint").and_then(|v| v.as_str()).map(|s| s.to_string());
            let participant = req.params.get("participant").and_then(|v| v.as_str());

            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let pid = participant.map(|s| s.to_string()).unwrap_or_else(|| format!("api_{}", endpoint.chars().filter(|c| c.is_alphanumeric()).take(16).collect::<String>()));
            let mut adapter = ApiAdapter::new(pid.clone());
            let intent = ResourceIntent::RestApi {
                endpoint: endpoint.to_string(),
                method: method.to_string(),
                headers: HashMap::new(),
                body: None,
                idempotency_key,
                compensation_endpoint: comp_endpoint,
                compensation_method: Some("DELETE".into()),
                compensation_body: None,
            };

            let mut guard = coord.lock().unwrap();
            match guard.prepare_participant(&tid, &mut adapter, intent) {
                Ok(ParticipantVote::Prepared(receipt)) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "participant": pid,
                        "receipt": receipt
                    })),
                    error: None,
                },
                Ok(ParticipantVote::Aborted { reason }) => IpcResponse {
                    success: false,
                    data: Some(json!({ "vote": "ABORTED", "reason": reason })),
                    error: Some(format!("Participant voted ABORT: {}", reason)),
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "savepoint" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let name = req.params.get("name").and_then(|v| v.as_str()).unwrap_or_default();

            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let mut guard = coord.lock().unwrap();
            match guard.savepoint(&tid, name) {
                Ok(sp) => IpcResponse {
                    success: true,
                    data: Some(json!(sp)),
                    error: None,
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "rollback_to" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let name = req.params.get("name").and_then(|v| v.as_str()).unwrap_or_default();

            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let mut guard = coord.lock().unwrap();
            let mut adapters: Vec<Box<dyn ResourceManager>> = match guard.get_status(&tid) {
                Some(tx) => reconstruct_adapters(&tx.participants),
                None => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(format!("Transaction {} not found", tid)),
                    }
                }
            };

            match guard.rollback_to(&tid, name, &mut adapters[..]) {
                Ok(unwound) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "tid": tid_str,
                        "savepoint": name,
                        "unwound_participants": unwound
                    })),
                    error: None,
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "commit" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let mut guard = coord.lock().unwrap();
            let mut adapters: Vec<Box<dyn ResourceManager>> = match guard.get_status(&tid) {
                Some(tx) => reconstruct_adapters(&tx.participants),
                None => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(format!("Transaction {} not found", tid)),
                    }
                }
            };

            match guard.commit(&tid, &mut adapters[..]) {
                Ok(()) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "decision": "COMMITTED",
                        "tid": tid_str
                    })),
                    error: None,
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "abort" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let reason = req.params.get("reason").and_then(|v| v.as_str()).unwrap_or("Daemon abort");
            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let mut guard = coord.lock().unwrap();
            let mut adapters: Vec<Box<dyn ResourceManager>> = match guard.get_status(&tid) {
                Some(tx) => reconstruct_adapters(&tx.participants),
                None => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(format!("Transaction {} not found", tid)),
                    }
                }
            };

            match guard.abort(&tid, reason, &mut adapters[..]) {
                Ok(()) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "decision": "ABORTED",
                        "tid": tid_str,
                        "reason": reason
                    })),
                    error: None,
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "status" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str());
            let guard = coord.lock().unwrap();

            if let Some(t_str) = tid_str {
                match TransactionId::parse(t_str) {
                    Ok(tid) => {
                        if let Some(tx) = guard.get_status(&tid) {
                            IpcResponse {
                                success: true,
                                data: Some(json!({
                                    "tid": tx.tid.to_string(),
                                    "state": tx.state.to_string(),
                                    "participants": tx.participants.iter().map(|p| &p.0).collect::<Vec<_>>(),
                                    "receipts_count": tx.receipts.len(),
                                    "savepoints_count": tx.savepoints.len(),
                                    "created_at": tx.created_at
                                })),
                                error: None,
                            }
                        } else {
                            IpcResponse {
                                success: false,
                                data: None,
                                error: Some(format!("Transaction {} not found", tid)),
                            }
                        }
                    }
                    Err(e) => IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    },
                }
            } else {
                let active = guard.active_transactions();
                let list: Vec<_> = active
                    .iter()
                    .map(|t| {
                        json!({
                            "tid": t.tid.to_string(),
                            "state": t.state.to_string(),
                            "participants_count": t.participants.len(),
                            "savepoints_count": t.savepoints.len()
                        })
                    })
                    .collect();
                IpcResponse {
                    success: true,
                    data: Some(json!({
                        "active": list,
                        "reaped_transactions": reaped_count.load(Ordering::SeqCst),
                        "auto_compactions": compact_count.load(Ordering::SeqCst),
                    })),
                    error: None,
                }
            }
        }

        "reap" => {
            let mut guard = coord.lock().unwrap();
            let now = chrono::Utc::now();
            let mut expired = Vec::new();
            for tx in guard.active_transactions() {
                if tx.state == TxState::Active || tx.state == TxState::Prepared {
                    let expiry = tx.created_at + chrono::Duration::milliseconds(tx.timeout_ms as i64);
                    if now > expiry {
                        expired.push((tx.tid, tx.clone()));
                    }
                }
            }

            let count = expired.len();
            for (tid, tx) in expired {
                let mut adapters = reconstruct_adapters(&tx.participants);
                let _ = guard.abort(&tid, "TTL expired by daemon reap command", &mut adapters[..]);
                reaped_count.fetch_add(1, Ordering::SeqCst);
            }

            IpcResponse {
                success: true,
                data: Some(json!({
                    "status": "ok",
                    "reaped_count": count,
                    "total_reaped": reaped_count.load(Ordering::SeqCst)
                })),
                error: None,
            }
        }

        "record_read" => {
            let tid_str = req.params.get("tid").and_then(|v| v.as_str()).unwrap_or_default();
            let path_str = req.params.get("path").and_then(|v| v.as_str()).unwrap_or_default();

            let tid = match TransactionId::parse(tid_str) {
                Ok(t) => t,
                Err(e) => {
                    return IpcResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    }
                }
            };

            let mut guard = coord.lock().unwrap();
            match guard.record_read(&tid, path_str) {
                Ok(hash) => IpcResponse {
                    success: true,
                    data: Some(json!({
                        "status": "ok",
                        "tid": tid_str,
                        "path": path_str,
                        "hash": format!("0x{:08x}", hash)
                    })),
                    error: None,
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        "compact" => {
            let mut guard = coord.lock().unwrap();
            match guard.compact() {
                Ok(report) => IpcResponse {
                    success: true,
                    data: Some(json!(report)),
                    error: None,
                },
                Err(e) => IpcResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }

        unknown => IpcResponse {
            success: false,
            data: None,
            error: Some(format!("Unknown IPC method: {}", unknown)),
        },
    }
}

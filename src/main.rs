//! agent-2pc CLI: Heterogeneous Two-Phase Commit Coordinator for AI Agent Workflows.

use agent_2pc::adapters::{ApiAdapter, CommandAdapter, FsAdapter, ResourceManager};
use agent_2pc::coordinator::Coordinator;
use agent_2pc::domain::{ParticipantVote, ResourceIntent, TransactionId, TxError};
use clap::{Parser, Subcommand};
use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "agent-2pc",
    author = "Ronit Gupta <ronitgupta138@gmail.com>",
    version = "0.1.0",
    about = "Two-Phase Commit (2PC) & Compensating Journaling Engine for AI Agent Tool Execution"
)]
struct Cli {
    /// Custom path to .agent-tx directory
    #[arg(long, default_value = ".agent-tx")]
    tx_dir: PathBuf,

    /// Machine-readable JSON output mode for agent harnesses
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize .agent-tx workspace in current directory
    Init,

    /// Begin a new transaction and get a unique Transaction ID (TID)
    Begin {
        /// Timeout in milliseconds (default: 30000)
        #[arg(long, default_value_t = 30_000)]
        timeout: u64,
    },

    /// Stage an atomic file write in Phase 1 (FsAdapter)
    StageWrite {
        /// Target transaction ID
        #[arg(long)]
        tid: String,

        /// Destination file path
        #[arg(long)]
        path: PathBuf,

        /// File content to write
        #[arg(long)]
        content: String,

        /// Create parent directories if missing
        #[arg(long, default_value_t = true)]
        create_dirs: bool,

        /// Optional custom participant ID
        #[arg(long)]
        participant: Option<String>,
    },

    /// Stage a file patch in Phase 1 (FsAdapter)
    StagePatch {
        /// Target transaction ID
        #[arg(long)]
        tid: String,

        /// Destination file path
        #[arg(long)]
        path: PathBuf,

        /// Patch diff content
        #[arg(long)]
        patch: String,

        /// Optional custom participant ID
        #[arg(long)]
        participant: Option<String>,
    },

    /// Stage and verify a shell command execution in Phase 1 (CommandAdapter)
    StageCmd {
        /// Target transaction ID
        #[arg(long)]
        tid: String,

        /// Shell command to execute
        #[arg(long)]
        command: String,

        /// Working directory (defaults to current dir)
        #[arg(long)]
        dir: Option<PathBuf>,

        /// Optional custom participant ID
        #[arg(long)]
        participant: Option<String>,
    },

    /// Stage an external REST API action with idempotency lease & compensation (ApiAdapter)
    StageApi {
        /// Target transaction ID
        #[arg(long)]
        tid: String,

        /// Target endpoint URL
        #[arg(long)]
        endpoint: String,

        /// HTTP Method (GET, POST, PUT, DELETE)
        #[arg(long, default_value = "POST")]
        method: String,

        /// Custom idempotency key (auto-generated if omitted)
        #[arg(long)]
        idempotency_key: Option<String>,

        /// Compensation rollback URL
        #[arg(long)]
        compensation_endpoint: Option<String>,

        /// Optional custom participant ID
        #[arg(long)]
        participant: Option<String>,
    },

    /// Stage command with sliding-window velocity tripwires (GovernorAdapter)
    StageGoverned {
        /// Target transaction ID
        #[arg(long)]
        tid: String,

        /// Command to execute
        #[arg(long)]
        command: String,

        /// Maximum concurrent tasks/threads before tripwire fires (default: 30)
        #[arg(long, default_value_t = 30)]
        max_forks: usize,

        /// Maximum runtime in milliseconds before freeze (default: 10000)
        #[arg(long, default_value_t = 10_000)]
        timeout_ms: u64,

        /// Optional custom participant ID
        #[arg(long)]
        participant: Option<String>,
    },

    /// Stage command execution inside hardware-isolated MicroVM boundary (MicrovmAdapter)
    StageVm {
        /// Target transaction ID
        #[arg(long)]
        tid: String,

        /// Command to execute
        #[arg(long)]
        command: String,

        /// Virtual CPUs allocated (default: 1)
        #[arg(long, default_value_t = 1)]
        vcpus: u8,

        /// Memory limit in Megabytes (default: 256)
        #[arg(long, default_value_t = 256)]
        memory_mb: u32,

        /// Optional custom participant ID
        #[arg(long)]
        participant: Option<String>,
    },

    /// Phase 2: Commit all staged changes atomically
    Commit {
        /// Target transaction ID
        #[arg(long)]
        tid: String,
    },

    /// Phase 2: Abort transaction and deterministically roll back staged changes
    Abort {
        /// Target transaction ID
        #[arg(long)]
        tid: String,

        /// Reason for abort
        #[arg(long, default_value = "Manual abort")]
        reason: String,
    },

    /// Inspect active transaction states and participants
    Status {
        /// Optional specific transaction ID
        #[arg(long)]
        tid: Option<String>,
    },

    /// Run crash recovery scan on WAL to resolve orphaned transactions
    Recover,

    /// Compact WAL by purging finalized transactions while preserving active in-flight state
    Compact,

    /// Execute a multi-step atomic transaction plan from a batch JSON manifest
    RunBatch {
        /// Path to batch transaction JSON file
        #[arg(long)]
        file: PathBuf,
    },

    /// Run native Model Context Protocol (MCP) JSON-RPC 2.0 stdio server
    Mcp,

    /// Record a file in the transaction's read-set for optimistic concurrency conflict detection
    RecordRead {
        /// Target transaction ID
        #[arg(long)]
        tid: String,

        /// Path to the read file
        #[arg(long)]
        path: PathBuf,
    },

    /// Display terminal live monitor and transaction HUD (alias: `top`)
    #[command(alias = "top")]
    Monitor {
        /// Continuously refresh and monitor in real-time
        #[arg(long, short)]
        watch: bool,

        /// Refresh interval in milliseconds (default: 1000)
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,
    },

    /// Perform 3-way semantic file merge for concurrent agent reconciliation
    Merge {
        /// Base common ancestor file
        #[arg(long)]
        base: PathBuf,

        /// Committed file version
        #[arg(long)]
        ours: PathBuf,

        /// Staged file version
        #[arg(long)]
        theirs: PathBuf,

        /// Destination output file (prints to stdout if omitted)
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[derive(Serialize, Deserialize)]
struct BatchStep {
    pub participant_id: String,
    pub intent: ResourceIntent,
}

#[derive(Serialize, Deserialize)]
struct BatchManifest {
    pub timeout_ms: Option<u64>,
    pub steps: Vec<BatchStep>,
}

fn main() {
    let cli = Cli::parse();

    if let Err(e) = run_cli(cli) {
        eprintln!("{} {}", "Error:".red().bold(), e);
        std::process::exit(1);
    }
}

fn run_cli(cli: Cli) -> Result<(), TxError> {
    let tx_dir = &cli.tx_dir;

    match cli.command {
        Commands::Init => {
            fs::create_dir_all(tx_dir)?;
            let _ = Coordinator::new(tx_dir)?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "status": "ok",
                        "tx_dir": tx_dir.display().to_string()
                    })
                );
            } else {
                println!(
                    "{} Initialized agent-2pc workspace at {}",
                    "✔".green().bold(),
                    tx_dir.display().to_string().bold()
                );
            }
        }

        Commands::Begin { timeout } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = coord.begin(Some(timeout))?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "status": "ok",
                        "tid": tid.to_string(),
                        "timeout_ms": timeout
                    })
                );
            } else {
                println!(
                    "{} Started transaction {} (timeout: {}ms)",
                    "✔".green().bold(),
                    tid.to_string().cyan().bold(),
                    timeout
                );
            }
        }

        Commands::StageWrite {
            tid,
            path,
            content,
            create_dirs,
            participant,
        } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = TransactionId::parse(&tid)?;
            let pid = participant.unwrap_or_else(|| {
                format!(
                    "fs_write_{}",
                    path.file_name().unwrap_or_default().to_string_lossy()
                )
            });

            let mut adapter = FsAdapter::new(pid);
            let intent = ResourceIntent::FsWrite {
                path: path.clone(),
                content,
                create_dirs,
            };

            let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
            match vote {
                ParticipantVote::Prepared(receipt) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "ok",
                                "vote": "PREPARED",
                                "receipt": receipt
                            })
                        );
                    } else {
                        println!(
                            "{} Staged write for {} (Participant: {})",
                            "✔ PREPARED".green().bold(),
                            path.display().to_string().bold(),
                            receipt.participant_id
                        );
                    }
                }
                ParticipantVote::Aborted { reason } => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "error",
                                "vote": "ABORTED",
                                "reason": reason
                            })
                        );
                    } else {
                        println!("{} {}", "✖ ABORTED:".red().bold(), reason);
                    }
                }
            }
        }

        Commands::StagePatch {
            tid,
            path,
            patch,
            participant,
        } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = TransactionId::parse(&tid)?;
            let pid = participant.unwrap_or_else(|| {
                format!(
                    "fs_patch_{}",
                    path.file_name().unwrap_or_default().to_string_lossy()
                )
            });

            let mut adapter = FsAdapter::new(pid);
            let intent = ResourceIntent::FsPatch {
                path: path.clone(),
                patch,
            };

            let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
            match vote {
                ParticipantVote::Prepared(receipt) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "ok",
                                "vote": "PREPARED",
                                "receipt": receipt
                            })
                        );
                    } else {
                        println!(
                            "{} Staged patch for {} (Participant: {})",
                            "✔ PREPARED".green().bold(),
                            path.display().to_string().bold(),
                            receipt.participant_id
                        );
                    }
                }
                ParticipantVote::Aborted { reason } => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "error",
                                "vote": "ABORTED",
                                "reason": reason
                            })
                        );
                    } else {
                        println!("{} {}", "✖ ABORTED:".red().bold(), reason);
                    }
                }
            }
        }

        Commands::StageCmd {
            tid,
            command,
            dir,
            participant,
        } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = TransactionId::parse(&tid)?;
            let pid = participant.unwrap_or_else(|| "cmd_exec".to_string());
            let working_dir = dir.unwrap_or_else(|| std::env::current_dir().unwrap_or_default());

            let mut adapter = CommandAdapter::new(pid);
            let intent = ResourceIntent::Command {
                command: command.clone(),
                working_dir,
                env: HashMap::new(),
            };

            let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
            match vote {
                ParticipantVote::Prepared(receipt) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "ok",
                                "vote": "PREPARED",
                                "receipt": receipt
                            })
                        );
                    } else {
                        println!(
                            "{} Command `{}` executed successfully in sandbox",
                            "✔ PREPARED".green().bold(),
                            command.cyan()
                        );
                    }
                }
                ParticipantVote::Aborted { reason } => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "error",
                                "vote": "ABORTED",
                                "reason": reason
                            })
                        );
                    } else {
                        println!("{} {}", "✖ ABORTED:".red().bold(), reason);
                    }
                }
            }
        }

        Commands::StageApi {
            tid,
            endpoint,
            method,
            idempotency_key,
            compensation_endpoint,
            participant,
        } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = TransactionId::parse(&tid)?;
            let key = idempotency_key.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let pid = participant.unwrap_or_else(|| "api_lease".to_string());

            let mut adapter = ApiAdapter::new(pid);
            let intent = ResourceIntent::RestApi {
                endpoint: endpoint.clone(),
                method,
                headers: HashMap::new(),
                body: None,
                idempotency_key: key.clone(),
                compensation_endpoint,
                compensation_method: Some("DELETE".to_string()),
                compensation_body: None,
            };

            let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
            match vote {
                ParticipantVote::Prepared(receipt) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "ok",
                                "vote": "PREPARED",
                                "idempotency_key": key,
                                "receipt": receipt
                            })
                        );
                    } else {
                        println!(
                            "{} Reserved API lease for {} (Idempotency Key: {})",
                            "✔ PREPARED".green().bold(),
                            endpoint.cyan(),
                            key.yellow()
                        );
                    }
                }
                ParticipantVote::Aborted { reason } => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "error",
                                "vote": "ABORTED",
                                "reason": reason
                            })
                        );
                    } else {
                        println!("{} {}", "✖ ABORTED:".red().bold(), reason);
                    }
                }
            }
        }

        Commands::StageGoverned {
            tid,
            command,
            max_forks,
            timeout_ms,
            participant,
        } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = TransactionId::parse(&tid)?;
            let pid = participant.unwrap_or_else(|| "gov_exec".to_string());

            let mut adapter = agent_2pc::governor::GovernorAdapter::new(pid);
            let intent = ResourceIntent::GovernedCommand {
                command: command.clone(),
                working_dir: std::env::current_dir().unwrap_or_default(),
                max_forks,
                timeout_ms,
                env: HashMap::new(),
            };

            let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
            match vote {
                ParticipantVote::Prepared(receipt) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "ok",
                                "vote": "PREPARED",
                                "receipt": receipt
                            })
                        );
                    } else {
                        println!(
                            "{} Governed execution `{}` passed velocity checks",
                            "✔ PREPARED".green().bold(),
                            command.cyan()
                        );
                    }
                }
                ParticipantVote::Aborted { reason } => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "error",
                                "vote": "ABORTED",
                                "reason": reason
                            })
                        );
                    } else {
                        println!("{} {}", "✖ ABORTED:".red().bold(), reason);
                    }
                }
            }
        }

        Commands::StageVm {
            tid,
            command,
            vcpus,
            memory_mb,
            participant,
        } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = TransactionId::parse(&tid)?;
            let pid = participant.unwrap_or_else(|| "vm_exec".to_string());

            let mut adapter = agent_2pc::microvm::MicrovmAdapter::new(pid);
            let intent = ResourceIntent::MicrovmCommand {
                command: command.clone(),
                working_dir: std::env::current_dir().unwrap_or_default(),
                vcpus,
                memory_mb,
                env: HashMap::new(),
            };

            let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
            match vote {
                ParticipantVote::Prepared(receipt) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "ok",
                                "vote": "PREPARED",
                                "receipt": receipt
                            })
                        );
                    } else {
                        println!(
                            "{} MicroVM execution `{}` prepared inside isolated boundary",
                            "✔ PREPARED".green().bold(),
                            command.cyan()
                        );
                    }
                }
                ParticipantVote::Aborted { reason } => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "error",
                                "vote": "ABORTED",
                                "reason": reason
                            })
                        );
                    } else {
                        println!("{} {}", "✖ ABORTED:".red().bold(), reason);
                    }
                }
            }
        }

        Commands::Commit { tid } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = TransactionId::parse(&tid)?;

            // Reconstruct resource adapters from transaction participants
            let tx = coord
                .get_status(&tid)
                .ok_or(TxError::TransactionNotFound(tid))?
                .clone();

            let mut adapters: Vec<Box<dyn ResourceManager>> = tx
                .participants
                .iter()
                .map(|p| -> Box<dyn ResourceManager> {
                    if p.0.starts_with("fs_") {
                        Box::new(FsAdapter::new(p.0.clone()))
                    } else if p.0.starts_with("cmd_") {
                        Box::new(CommandAdapter::new(p.0.clone()))
                    } else {
                        Box::new(ApiAdapter::new(p.0.clone()))
                    }
                })
                .collect();

            coord.commit(&tid, &mut adapters[..])?;

            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "status": "ok",
                        "decision": "COMMITTED",
                        "tid": tid.to_string()
                    })
                );
            } else {
                println!(
                    "{} Transaction {} COMMITTED atomically",
                    "✔ 2PC SUCCESS:".green().bold(),
                    tid.to_string().cyan().bold()
                );
            }
        }

        Commands::Abort { tid, reason } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = TransactionId::parse(&tid)?;

            let tx = coord
                .get_status(&tid)
                .ok_or(TxError::TransactionNotFound(tid))?
                .clone();

            let mut adapters: Vec<Box<dyn ResourceManager>> = tx
                .participants
                .iter()
                .map(|p| -> Box<dyn ResourceManager> {
                    if p.0.starts_with("fs_") {
                        Box::new(FsAdapter::new(p.0.clone()))
                    } else if p.0.starts_with("cmd_") {
                        Box::new(CommandAdapter::new(p.0.clone()))
                    } else {
                        Box::new(ApiAdapter::new(p.0.clone()))
                    }
                })
                .collect();

            coord.abort(&tid, &reason, &mut adapters[..])?;

            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "status": "ok",
                        "decision": "ABORTED",
                        "tid": tid.to_string(),
                        "reason": reason
                    })
                );
            } else {
                println!(
                    "{} Transaction {} ABORTED and rolled back cleanly: {}",
                    "✔ 2PC ROLLBACK:".yellow().bold(),
                    tid.to_string().cyan().bold(),
                    reason
                );
            }
        }

        Commands::Status { tid } => {
            let coord = Coordinator::new(tx_dir)?;
            if let Some(t_str) = tid {
                let tid = TransactionId::parse(&t_str)?;
                if let Some(tx) = coord.get_status(&tid) {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "tid": tx.tid.to_string(),
                                "state": tx.state.to_string(),
                                "participants": tx.participants.iter().map(|p| &p.0).collect::<Vec<_>>(),
                                "receipts_count": tx.receipts.len(),
                                "created_at": tx.created_at
                            })
                        );
                    } else {
                        println!("Transaction: {}", tx.tid.to_string().cyan().bold());
                        println!("Status:      {}", tx.state.to_string().bold());
                        println!("Participants: {:?}", tx.participants);
                        println!("Prepared:    {}/{}", tx.receipts.len(), tx.participants.len());
                    }
                } else {
                    eprintln!("Transaction not found: {}", t_str);
                }
            } else {
                let active = coord.active_transactions();
                if cli.json {
                    let list: Vec<_> = active
                        .iter()
                        .map(|t| {
                            serde_json::json!({
                                "tid": t.tid.to_string(),
                                "state": t.state.to_string(),
                                "participants_count": t.participants.len()
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&list)?);
                } else {
                    println!("{} Active Transactions:", "Active".bold());
                    for t in active {
                        println!(" - {} [{}]", t.tid.to_string().cyan(), t.state);
                    }
                }
            }
        }

        Commands::Recover => {
            let mut coord = Coordinator::new(tx_dir)?;
            let mut dummy_adapters: Vec<Box<dyn ResourceManager>> = Vec::new();
            let report = coord.recover(&mut dummy_adapters[..])?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{} Crash Recovery Complete", "✔".green().bold());
                println!("Scanned Records: {}", report.total_records_scanned);
                println!("Rolled Back:     {}", report.rolled_back.len());
                println!("Rolled Forward:  {}", report.rolled_forward.len());
            }
        }

        Commands::Compact => {
            let mut coord = Coordinator::new(tx_dir)?;
            let report = coord.compact()?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{} WAL Compaction Complete", "✔".green().bold());
                println!("Original Size:        {} bytes ({} records)", report.original_bytes, report.original_records);
                println!("Compacted Size:       {} bytes ({} records)", report.compacted_bytes, report.compacted_records);
                let saved = report.original_bytes.saturating_sub(report.compacted_bytes);
                let pct = if report.original_bytes > 0 {
                    (saved as f64 / report.original_bytes as f64) * 100.0
                } else {
                    0.0
                };
                println!("Disk Space Reclaimed: {} bytes ({:.1}%)", saved, pct);
                println!("Active Preserved:     {}", report.active_transactions);
                println!("Finalized Purged:     {}", report.purged_transactions);
            }
        }

        Commands::RunBatch { file } => {
            let data = fs::read_to_string(&file)?;
            let manifest: BatchManifest = serde_json::from_str(&data)?;

            let mut coord = Coordinator::new(tx_dir)?;
            let tid = coord.begin(manifest.timeout_ms)?;

            println!(
                "{} Started Batch 2PC Transaction {}",
                "⚡".yellow().bold(),
                tid.to_string().cyan().bold()
            );

            let mut adapters: Vec<Box<dyn ResourceManager>> = Vec::new();
            let mut all_prepared = true;
            let mut failure_reason = String::new();

            for step in &manifest.steps {
                let mut adapter: Box<dyn ResourceManager> = match &step.intent {
                    ResourceIntent::FsWrite { .. } | ResourceIntent::FsPatch { .. } => {
                        Box::new(FsAdapter::new(step.participant_id.clone()))
                    }
                    ResourceIntent::Command { .. } => {
                        Box::new(CommandAdapter::new(step.participant_id.clone()))
                    }
                    ResourceIntent::RestApi { .. } => {
                        Box::new(ApiAdapter::new(step.participant_id.clone()))
                    }
                    _ => Box::new(FsAdapter::new(step.participant_id.clone())),
                };

                let vote =
                    coord.prepare_participant(&tid, adapter.as_mut(), step.intent.clone())?;

                match vote {
                    ParticipantVote::Prepared(_) => {
                        println!(
                            "  [Phase 1] {} vote: {}",
                            step.participant_id.bold(),
                            "PREPARED".green().bold()
                        );
                        adapters.push(adapter);
                    }
                    ParticipantVote::Aborted { reason } => {
                        println!(
                            "  [Phase 1] {} vote: {} ({})",
                            step.participant_id.bold(),
                            "ABORT".red().bold(),
                            reason
                        );
                        adapters.push(adapter);
                        all_prepared = false;
                        failure_reason = reason;
                        break;
                    }
                }
            }

            if all_prepared {
                coord.commit(&tid, &mut adapters[..])?;
                println!(
                    "{} [Phase 2] All steps passed verification. COMMITTED atomically.",
                    "✔ 2PC SUCCESS:".green().bold()
                );
            } else {
                coord.abort(&tid, &failure_reason, &mut adapters[..])?;
                println!(
                    "{} [Phase 2] Step verification failed. Rolled back all changes without leaks.",
                    "✔ 2PC ROLLBACK:".yellow().bold()
                );
            }
        }

        Commands::Mcp => {
            let mcp = agent_2pc::mcp::McpServer::new(tx_dir);
            let stdin = std::io::stdin();
            let stdout = std::io::stdout();
            mcp.run_loop(stdin.lock(), stdout.lock())?;
        }

        Commands::RecordRead { tid, path } => {
            let mut coord = Coordinator::new(tx_dir)?;
            let tid = TransactionId::parse(&tid)?;
            let hash = coord.record_read(&tid, &path)?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "status": "ok",
                        "tid": tid.to_string(),
                        "path": path.display().to_string(),
                        "hash": format!("0x{:08x}", hash)
                    })
                );
            } else {
                println!(
                    "{} Recorded read-set invariant for {} (hash: 0x{:08x})",
                    "✔".green().bold(),
                    path.display().to_string().bold(),
                    hash
                );
            }
        }

        Commands::Monitor { watch, interval_ms } => {
            let monitor = agent_2pc::monitor::Monitor::new(tx_dir);
            if cli.json {
                let stats = monitor.collect_stats()?;
                println!("{}", serde_json::to_string_pretty(&stats)?);
            } else if watch {
                monitor.watch_loop(interval_ms)?;
            } else {
                monitor.render_terminal()?;
            }
        }

        Commands::Merge {
            base,
            ours,
            theirs,
            output,
        } => {
            let base_str = fs::read_to_string(&base)?;
            let ours_str = fs::read_to_string(&ours)?;
            let theirs_str = fs::read_to_string(&theirs)?;

            let merge_res = agent_2pc::merge::ThreeWayMerger::merge(&base_str, &ours_str, &theirs_str);
            match merge_res {
                agent_2pc::merge::MergeResult::Clean(merged) => {
                    if let Some(out_path) = output {
                        fs::write(&out_path, &merged)?;
                        println!("✔ Clean merge written to {}", out_path.display());
                    } else if cli.json {
                        println!("{}", serde_json::json!({
                            "status": "clean",
                            "merged": merged
                        }));
                    } else {
                        println!("{}", merged);
                    }
                }
                agent_2pc::merge::MergeResult::Conflict {
                    merged_with_markers,
                    conflict_count,
                } => {
                    if cli.json {
                        println!("{}", serde_json::json!({
                            "status": "conflict",
                            "conflict_count": conflict_count,
                            "merged": merged_with_markers
                        }));
                    } else {
                        eprintln!("✖ Merge conflict ({} conflicts detected)", conflict_count);
                        println!("{}", merged_with_markers);
                    }
                }
            }
        }
    }

    Ok(())
}

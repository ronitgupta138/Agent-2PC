import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

/**
 * Options for configuring transaction initiation.
 */
export interface TransactionOptions {
  /** Transaction timeout in milliseconds (default: 30000). */
  timeoutMs?: number;
}

/**
 * Options for staging REST API external operations.
 */
export interface ApiOptions {
  /** HTTP method: GET, POST, PUT, DELETE (default: POST). */
  method?: string;
  /** Custom idempotency key to prevent double execution. */
  idempotencyKey?: string;
  /** Compensation rollback endpoint URL invoked on transaction abort. */
  compensationEndpoint?: string;
  /** Optional custom participant identifier. */
  participant?: string;
}

/**
 * Output format for diff queries.
 */
export interface DiffFile {
  path: string;
  is_new: boolean;
  is_deleted: boolean;
  additions: number;
  deletions: number;
  unified_diff: string;
}

export interface DiffResult {
  tid: string;
  files?: DiffFile[];
  total_additions?: number;
  total_deletions?: number;
}

/**
 * Result returned when exporting a transaction bundle.
 */
export interface BundleExportResult {
  tid: string;
  output_path: string;
  participant_count: number;
  file_count: number;
  bytes_written: number;
  crc32: number;
}

/**
 * Result returned when importing a transaction bundle.
 */
export interface BundleImportResult {
  tid: string;
  source_path: string;
  participant_count: number;
  restored_files: number;
  status: string;
}

/**
 * Result of atomic 2PC git commit.
 */
export interface GitCommitResult {
  tid: string;
  commit_hash: string;
  files_committed: string[];
  message: string;
}

/**
 * Git repository integrity check status report.
 */
export interface HookCheckResult {
  clean: boolean;
  in_flight_tids: string[];
  message: string;
}

/**
 * Hook installation status report.
 */
export interface HookInstallResult {
  hook_path: string;
  installed: boolean;
  message: string;
}

/**
 * Active transaction session bound to a specific Transaction ID (TID).
 */
export class TxSession {
  public committed = false;
  public aborted = false;

  constructor(
    public readonly client: Agent2PC,
    public readonly tid: string
  ) {}

  /**
   * Phase 1: Stage file write with .bak snapshotting into isolated scratch memory.
   */
  public async stageWrite(
    path: string,
    content: string,
    createDirs = true,
    participant?: string
  ): Promise<any> {
    return this.client.stageWrite(this.tid, path, content, createDirs, participant);
  }

  /**
   * Phase 1: Stage a unified diff patch in scratch memory.
   */
  public async stagePatch(
    path: string,
    patch: string,
    participant?: string
  ): Promise<any> {
    return this.client.stagePatch(this.tid, path, patch, participant);
  }

  /**
   * Phase 1: Stage targeted unique substring replacement with uniqueness validation.
   */
  public async stageReplace(
    path: string,
    oldString: string,
    newString: string,
    participant?: string
  ): Promise<any> {
    return this.client.stageReplace(this.tid, path, oldString, newString, participant);
  }

  /**
   * Phase 1: Stage and verify a shell command in an isolated scratch sandbox.
   */
  public async stageCmd(
    command: string,
    dir?: string,
    participant?: string
  ): Promise<any> {
    return this.client.stageCmd(this.tid, command, dir, participant);
  }

  /**
   * Alias for stageCmd.
   */
  public async stageCommand(
    command: string,
    dir?: string,
    participant?: string
  ): Promise<any> {
    return this.stageCmd(command, dir, participant);
  }

  /**
   * Phase 1: Stage external REST API action with idempotency lease & compensation hook.
   */
  public async stageApi(
    endpoint: string,
    options?: ApiOptions
  ): Promise<any> {
    return this.client.stageApi(this.tid, endpoint, options);
  }

  /**
   * Register file fingerprint in read-set for optimistic concurrency conflict detection (OCC).
   */
  public async recordRead(path: string): Promise<string> {
    return this.client.recordRead(this.tid, path);
  }

  /**
   * Mark a named transactional checkpoint for speculative execution and sub-task branching.
   */
  public async savepoint(name: string): Promise<any> {
    return this.client.savepoint(this.tid, name);
  }

  /**
   * Unwind staged steps back to a named savepoint without aborting the entire transaction.
   */
  public async rollbackTo(name: string): Promise<any> {
    return this.client.rollbackTo(this.tid, name);
  }

  /**
   * Inspect unified diff or summary diffstat of staged changes in this transaction.
   */
  public async diff(stat?: boolean): Promise<DiffResult> {
    return this.client.diff(this.tid, stat);
  }

  /**
   * Export active transaction and all staged artifacts to a portable verified .a2pc bundle.
   */
  public async exportBundle(output?: string): Promise<BundleExportResult> {
    return this.client.exportBundle(this.tid, output);
  }

  /**
   * Atomically commit 2PC transaction and create a Git commit in one unified step.
   */
  public async gitCommit(message: string, dir?: string): Promise<GitCommitResult> {
    const res = await this.client.gitCommit(this.tid, message, dir);
    this.committed = true;
    return res;
  }

  /**
   * Execute arbitrary command and auto-stage any created or modified files into Phase 1.
   */
  public async exec(command: string[], workingDir?: string): Promise<any> {
    return this.client.exec(this.tid, command, workingDir);
  }

  /**
   * Inspect active transaction state, receipts, and enrolled participants.
   */
  public async status(): Promise<any> {
    return this.client.status(this.tid);
  }

  /**
   * Phase 2: Atomically commit all staged operations across all participants.
   */
  public async commit(): Promise<any> {
    if (this.committed || this.aborted) {
      throw new Error("Transaction already finalized");
    }
    const res = await this.client.commit(this.tid);
    this.committed = true;
    return res;
  }

  /**
   * Phase 2: Deterministically roll back all staged changes and restore backups.
   */
  public async abort(reason = "Client abort"): Promise<any> {
    if (this.committed || this.aborted) {
      return {};
    }
    const res = await this.client.abort(this.tid, reason);
    this.aborted = true;
    return res;
  }
}

/**
 * Client for driving the agent-2pc Two-Phase Commit transaction coordinator.
 */
export class Agent2PC {
  constructor(
    public readonly binaryPath = "agent-2pc",
    public readonly txDir = ".agent-tx"
  ) {}

  /**
   * Low-level helper to execute agent-2pc CLI commands with JSON output.
   */
  public async execute(args: string[]): Promise<any> {
    const fullArgs = ["--tx-dir", this.txDir, "--json", ...args];
    try {
      const { stdout, stderr } = await execFileAsync(this.binaryPath, fullArgs);
      const text = stdout ? stdout.trim() : (stderr ? stderr.trim() : "");
      if (!text) {
        return {};
      }
      try {
        return JSON.parse(text);
      } catch {
        return { output: text };
      }
    } catch (err: any) {
      const errMsg = err?.stderr?.trim() || err?.stdout?.trim() || err?.message || String(err);
      throw new Error(`agent-2pc error: ${errMsg}`);
    }
  }

  /**
   * Initialize .agent-tx workspace in current directory.
   */
  public async init(): Promise<any> {
    return this.execute(["init"]);
  }

  /**
   * Start a new transaction session and obtain a unique TID.
   */
  public async begin(options: TransactionOptions = {}): Promise<TxSession> {
    const timeout = options.timeoutMs ?? 30000;
    const res = await this.execute(["begin", "--timeout", String(timeout)]);
    if (!res.tid) {
      throw new Error("Failed to initialize transaction");
    }
    return new TxSession(this, res.tid);
  }

  /**
   * Inspect active transaction states and participants.
   */
  public async status(tid?: string): Promise<any> {
    const args = ["status"];
    if (tid) {
      args.push("--tid", tid);
    }
    return this.execute(args);
  }

  /**
   * Stage an atomic file write in Phase 1 (FsAdapter).
   */
  public async stageWrite(
    tid: string,
    path: string,
    content: string,
    createDirs = true,
    participant?: string
  ): Promise<any> {
    const args = [
      "stage-write",
      "--tid", tid,
      "--path", path,
      "--content", content,
    ];
    if (createDirs) {
      args.push("--create-dirs");
    }
    if (participant) {
      args.push("--participant", participant);
    }
    return this.execute(args);
  }

  /**
   * Stage a file patch in Phase 1 (FsAdapter).
   */
  public async stagePatch(
    tid: string,
    path: string,
    patch: string,
    participant?: string
  ): Promise<any> {
    const args = [
      "stage-patch",
      "--tid", tid,
      "--path", path,
      `--patch=${patch}`,
    ];
    if (participant) {
      args.push("--participant", participant);
    }
    return this.execute(args);
  }

  /**
   * Stage a targeted substring replacement in Phase 1 (FsAdapter).
   */
  public async stageReplace(
    tid: string,
    path: string,
    oldString: string,
    newString: string,
    participant?: string
  ): Promise<any> {
    const args = [
      "stage-replace",
      "--tid", tid,
      "--path", path,
      "--old-string", oldString,
      "--new-string", newString,
    ];
    if (participant) {
      args.push("--participant", participant);
    }
    return this.execute(args);
  }

  /**
   * Stage and verify a shell command execution in Phase 1 (CommandAdapter).
   */
  public async stageCmd(
    tid: string,
    command: string,
    dir?: string,
    participant?: string
  ): Promise<any> {
    const args = ["stage-cmd", "--tid", tid, "--command", command];
    if (dir) {
      args.push("--dir", dir);
    }
    if (participant) {
      args.push("--participant", participant);
    }
    return this.execute(args);
  }

  /**
   * Alias for stageCmd.
   */
  public async stageCommand(
    tid: string,
    command: string,
    dir?: string,
    participant?: string
  ): Promise<any> {
    return this.stageCmd(tid, command, dir, participant);
  }

  /**
   * Stage an external REST API action with idempotency lease & compensation (ApiAdapter).
   */
  public async stageApi(
    tid: string,
    endpoint: string,
    options: ApiOptions = {}
  ): Promise<any> {
    const method = options.method ?? "POST";
    const args = ["stage-api", "--tid", tid, "--endpoint", endpoint, "--method", method];
    if (options.idempotencyKey) {
      args.push("--idempotency-key", options.idempotencyKey);
    }
    if (options.compensationEndpoint) {
      args.push("--compensation-endpoint", options.compensationEndpoint);
    }
    if (options.participant) {
      args.push("--participant", options.participant);
    }
    return this.execute(args);
  }

  /**
   * Record a file in the transaction's read-set for optimistic concurrency conflict detection.
   */
  public async recordRead(tid: string, path: string): Promise<string> {
    const res = await this.execute(["record-read", "--tid", tid, "--path", path]);
    return res.hash || "";
  }

  /**
   * Phase 2: Commit all staged changes atomically.
   */
  public async commit(tid: string): Promise<any> {
    return this.execute(["commit", "--tid", tid]);
  }

  /**
   * Phase 2: Abort transaction and deterministically roll back staged changes.
   */
  public async abort(tid: string, reason = "Client abort"): Promise<any> {
    return this.execute(["abort", "--tid", tid, "--reason", reason]);
  }

  /**
   * Create a transactional savepoint checkpoint within an active transaction.
   */
  public async savepoint(tid: string, name: string): Promise<any> {
    return this.execute(["savepoint", "--tid", tid, "--name", name]);
  }

  /**
   * Roll back transaction changes staged after the specified savepoint.
   */
  public async rollbackTo(tid: string, name: string): Promise<any> {
    return this.execute(["rollback-to", "--tid", tid, "--name", name]);
  }

  /**
   * Execute arbitrary command and auto-stage any created or modified files into Phase 1.
   */
  public async exec(tid: string, command: string[], workingDir?: string): Promise<any> {
    const args = ["exec", "--tid", tid];
    if (workingDir) {
      args.push("--dir", workingDir);
    }
    args.push("--", ...command);
    return this.execute(args);
  }

  /**
   * Display unified diff of all staged file modifications in an active transaction.
   */
  public async diff(tid: string, stat?: boolean): Promise<DiffResult> {
    const args = ["diff", "--tid", tid];
    if (stat) {
      args.push("--stat");
    }
    return this.execute(args);
  }

  /**
   * Export an active transaction to a portable .a2pc bundle.
   */
  public async exportBundle(tid: string, output?: string): Promise<BundleExportResult> {
    const args = ["export", "--tid", tid];
    if (output) {
      args.push("--output", output);
    }
    return this.execute(args);
  }

  /**
   * Import a transaction bundle (.a2pc) and register as a prepared transaction.
   */
  public async importBundle(file: string): Promise<BundleImportResult> {
    return this.execute(["import", "--file", file]);
  }

  /**
   * Atomically commit 2PC transaction and create a Git commit in one step.
   */
  public async gitCommit(tid: string, message: string, dir?: string): Promise<GitCommitResult> {
    const args = ["git-commit", "--tid", tid, `--message=${message}`];
    if (dir) {
      args.push("--dir", dir);
    }
    return this.execute(args);
  }

  /**
   * Check whether .agent-tx is clean of pending uncommitted transactions.
   */
  public async hookCheck(): Promise<HookCheckResult> {
    return this.execute(["hook", "check"]);
  }

  /**
   * Install pre-commit repository integrity hook into .git/hooks.
   */
  public async hookInstall(repoDir = "."): Promise<HookInstallResult> {
    return this.execute(["hook", "install", "--repo-dir", repoDir]);
  }

  /**
   * Trigger TTL sweep of expired transactions via daemon IPC.
   */
  public async daemonReap(): Promise<any> {
    return this.execute(["daemon", "reap"]);
  }

  /**
   * Start the agent-2pc daemon.
   */
  public async daemonStart(foreground = false): Promise<any> {
    const args = ["daemon", "start"];
    if (foreground) {
      args.push("--foreground");
    }
    return this.execute(args);
  }

  /**
   * Stop the running agent-2pc daemon.
   */
  public async daemonStop(): Promise<any> {
    return this.execute(["daemon", "stop"]);
  }

  /**
   * Query status of the agent-2pc daemon.
   */
  public async daemonStatus(): Promise<any> {
    return this.execute(["daemon", "status"]);
  }

  /**
   * Run crash recovery scan on WAL to resolve orphaned transactions.
   */
  public async recover(): Promise<any> {
    return this.execute(["recover"]);
  }

  /**
   * Compact WAL by purging finalized transactions while preserving active in-flight state.
   */
  public async compact(): Promise<any> {
    return this.execute(["compact"]);
  }
}

import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export interface TransactionOptions {
  timeoutMs?: number;
}

export class TxSession {
  constructor(
    private readonly client: Agent2PC,
    public readonly tid: string
  ) {}

  public async stageWrite(path: string, content: string, createDirs = true): Promise<any> {
    return this.client.execute([
      "stage-write",
      "--tid", this.tid,
      "--path", path,
      "--content", content,
      "--create-dirs", String(createDirs)
    ]);
  }

  public async stageCommand(command: string): Promise<any> {
    return this.client.execute([
      "stage-cmd",
      "--tid", this.tid,
      "--command", command
    ]);
  }

  public async recordRead(path: string): Promise<string> {
    const res = await this.client.execute([
      "record-read",
      "--tid", this.tid,
      "--path", path
    ]);
    return res.hash || "";
  }

  public async savepoint(name: string): Promise<any> {
    return this.client.execute([
      "savepoint",
      "--tid", this.tid,
      "--name", name
    ]);
  }

  public async rollbackTo(name: string): Promise<any> {
    return this.client.execute([
      "rollback-to",
      "--tid", this.tid,
      "--name", name
    ]);
  }

  public async exec(command: string[], workingDir?: string): Promise<any> {
    const args = ["exec", "--tid", this.tid];
    if (workingDir) {
      args.push("--dir", workingDir);
    }
    args.push("--", ...command);
    return this.client.execute(args);
  }

  public async commit(): Promise<any> {
    return this.client.execute(["commit", "--tid", this.tid]);
  }

  public async abort(reason = "Client abort"): Promise<any> {
    return this.client.execute(["abort", "--tid", this.tid, "--reason", reason]);
  }
}

export class Agent2PC {
  constructor(
    private readonly binaryPath = "agent-2pc",
    private readonly txDir = ".agent-tx"
  ) {}

  public async execute(args: string[]): Promise<any> {
    const fullArgs = ["--tx-dir", this.txDir, "--json", ...args];
    const { stdout, stderr } = await execFileAsync(this.binaryPath, fullArgs);
    if (stderr && !stdout) {
      throw new Error(`agent-2pc error: ${stderr.trim()}`);
    }
    try {
      return JSON.parse(stdout.trim());
    } catch {
      return { output: stdout.trim() };
    }
  }

  public async begin(options: TransactionOptions = {}): Promise<TxSession> {
    const timeout = options.timeoutMs ?? 30000;
    const res = await this.execute(["begin", "--timeout", String(timeout)]);
    if (!res.tid) {
      throw new Error("Failed to initialize transaction");
    }
    return new TxSession(this, res.tid);
  }
}

/**
 * agent-2pc v2.0.0 TypeScript Workflow Example.
 *
 * Demonstrates:
 * 1. Initializing workspace
 * 2. Repository integrity check
 * 3. Beginning transaction and obtaining TID
 * 4. Staging atomic file modifications
 * 5. Checking savepoints and rollback
 * 6. Inspecting unified diff
 * 7. Exporting and importing .a2pc bundles
 * 8. Committing transaction
 */

import { Agent2PC } from "../sdk/typescript/dist/index.js";
import * as path from "node:path";
import * as fs from "node:fs";

async function main() {
  const client = new Agent2PC("agent-2pc", ".agent-tx");

  console.log("1. Initializing workspace...");
  const initRes = await client.init();
  console.log("   Workspace initialized:", initRes.status);

  console.log("2. Checking repository pre-commit integrity...");
  const hookCheck = await client.hookCheck();
  console.log("   Clean repository state:", hookCheck.clean);

  console.log("3. Beginning transaction...");
  const session = await client.begin({ timeoutMs: 30000 });
  console.log("   Transaction ID:", session.tid);

  // 1. OCC read record
  const readmePath = "README.md";
  if (fs.existsSync(readmePath)) {
    const hash = await session.recordRead(readmePath);
    console.log(`   OCC fingerprint for ${readmePath}:`, hash);
  }

  // 2. Stage file write
  const targetFile = "examples/tmp_ts_sample.txt";
  await session.stageWrite(targetFile, "TypeScript SDK 2PC Staging\n");
  console.log("   Staged file write (FsAdapter).");

  // 3. Mark savepoint
  await session.savepoint("checkpoint_1");
  console.log("   Created savepoint 'checkpoint_1'.");

  // 4. Stage command verification
  const cmdRes = await session.stageCmd("echo 'Verification step passed'");
  console.log("   Sandbox command vote:", cmdRes.vote);

  // 5. Inspect diff
  const diffReport = await session.diff();
  console.log("   Staged files in diff:", diffReport.files?.length ?? 0);

  // 6. Roll back speculative command to savepoint
  await session.rollbackTo("checkpoint_1");
  console.log("   Rolled back to 'checkpoint_1'.");

  // 7. Export transaction bundle
  const bundlePath = "examples/ts_sample.a2pc";
  const expRes = await session.exportBundle(bundlePath);
  console.log(`   Exported transaction bundle (${expRes.bytes_written} bytes).`);

  // 8. Commit transaction
  await session.commit();
  console.log("   Transaction committed atomically.");

  // Clean up demo files
  if (fs.existsSync(targetFile)) {
    fs.unlinkSync(targetFile);
  }
  if (fs.existsSync(bundlePath)) {
    fs.unlinkSync(bundlePath);
  }

  console.log("✔ TypeScript workflow completed successfully!");
}

main().catch((err) => {
  console.error("Workflow failed:", err);
  process.exit(1);
});

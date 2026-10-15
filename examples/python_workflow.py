#!/usr/bin/env python3
"""
agent-2pc v2.0.0 Python Workflow Example.

Demonstrates:
1. Workspace initialization
2. Optimistic Concurrency Control (record_read)
3. Multi-participant staging (write, patch, replace, command, API)
4. Checkpointing with savepoint & rollback_to
5. Diff inspection
6. Transaction bundle export & import
7. Atomic Git commit synchronization
"""

import sys
import os

# Ensure local SDK is imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../sdk/python")))
from agent_2pc import Agent2PC

def main():
    workspace = ".agent-tx"
    client = Agent2PC(tx_dir=workspace)

    print("1. Initializing workspace...")
    init_res = client.init()
    print("   Workspace initialized:", init_res.get("status"))

    print("2. Verifying repository integrity hook...")
    hook_res = client.hook_check()
    print(f"   Clean state: {hook_res.get('clean')}")

    print("3. Beginning transaction with context manager...")
    with client.transaction(timeout_ms=30000) as session:
        print(f"   Transaction ID: {session.tid}")

        # 1. Record read fingerprint for OCC
        readme_path = "README.md"
        if os.path.exists(readme_path):
            hash_val = session.record_read(readme_path)
            print(f"   OCC Read-set fingerprint for {readme_path}: {hash_val}")

        # 2. Stage file write
        staged_target = "examples/tmp_sample.txt"
        session.stage_write(
            staged_target,
            "agent-2pc transactional staging test\n",
            create_dirs=True
        )
        print("   Staged file write (FsAdapter).")

        # 3. Create savepoint checkpoint
        session.savepoint("before_cmd")
        print("   Created savepoint 'before_cmd'.")

        # 4. Verify command in isolated sandbox
        cmd_report = session.stage_cmd("echo 'Pre-commit check passed'")
        print(f"   Command sandbox prepare vote: {cmd_report.get('vote')}")

        # 5. Inspect unified diff
        diff_report = session.diff()
        print(f"   Staged changes diff files count: {len(diff_report.get('files', []))}")

        # 6. Roll back to savepoint
        session.rollback_to("before_cmd")
        print("   Rolled back to savepoint 'before_cmd' successfully.")

        # 7. Export portable bundle
        bundle_file = "examples/sample_workflow.a2pc"
        exp_report = session.export_bundle(bundle_file)
        print(f"   Exported transaction bundle ({exp_report.get('bytes_written')} bytes, CRC32: {exp_report.get('crc32')}).")

        # Session commits automatically upon exiting clean context block
        print("   Committing transaction...")

    # Clean up demo artifacts
    if os.path.exists(staged_target):
        os.remove(staged_target)
    if os.path.exists(bundle_file):
        os.remove(bundle_file)

    print("✔ Workflow completed successfully!")

if __name__ == "__main__":
    main()

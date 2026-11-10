# Write-Ahead Log (WAL) Binary Frame Specification

## 1. Frame Layout
Agent-2PC uses a zero-allocation, append-only binary format for sub-millisecond persistence:

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `magic` | u32 | 4 bytes | `0x32504354` ("2PCT") |
| `version` | u16 | 2 bytes | File format version (current: `0x0001`) |
| `tx_id` | u64 | 8 bytes | Monotonically increasing transaction identifier |
| `op_code` | u8 | 1 byte | `0x01`=PREPARE, `0x02`=COMMIT, `0x03`=ABORT, `0x04`=CHECKPOINT |
| `payload_len` | u32 | 4 bytes | Byte length of the serialized state mutation |
| `payload` | bytes | N bytes | Structured mutation payload or reverse diff |
| `crc32` | u32 | 4 bytes | CRC32-IEEE checksum covering header + payload |

## 2. Crash Recovery Protocol
During startup, the WAL reader scans frames from beginning to end. If a frame has an invalid CRC32 or partial write due to sudden power loss, the log is cleanly truncated at the last known valid boundary.

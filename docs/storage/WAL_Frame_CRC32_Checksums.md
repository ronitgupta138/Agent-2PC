# Write-Ahead Log (WAL) Binary Framing & CRC32 Specification

## 1. Binary Frame Layout
Each transaction log entry written to durable storage is structured as a length-prefixed binary frame:

```
+---------------+----------------+------------------+----------------+
| Length (4B)   | FrameType (1B) | Payload (NB)     | CRC32 (4B)     |
+---------------+----------------+------------------+----------------+
```

## 2. Checksum Verification
- Computed via IEEE 802.3 polynomial over `FrameType + Payload`.
- On crash recovery, frames with corrupt checksums are truncated at the last known valid boundary, preventing corrupted partial writes from replaying.

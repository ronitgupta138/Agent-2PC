# gRPC Inter-Agent Consensus Channel Mutual TLS (mTLS) Specification

## 1. Security Architecture
Consensus voting messages (`VOTE_COMMIT`, `VOTE_ABORT`, `GLOBAL_COMMIT`) transmit over HTTP/2 using gRPC secured with mutual TLS.

## 2. Cryptographic Configuration
- **Protocol:** TLS 1.3 only (cipher suites: `TLS_AES_256_GCM_SHA384`, `TLS_CHACHA20_POLY1305_SHA256`).
- **Mutual Certificate Validation:** Every participating agent validates peer X.509 certificates against an internal cluster Certificate Authority (CA).

## 3. Replay Protection
Each consensus frame contains a monotonically increasing 64-bit sequence number and nonce, preventing network replay attacks.

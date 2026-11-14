use crc32fast::Hasher;

/// Compute standard IEEE 802.3 CRC32 checksum over a binary byte buffer.
pub fn compute_frame_crc32(payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(payload);
    hasher.finalize()
}

/// Verify that payload matches expected CRC32 checksum.
pub fn verify_frame_crc32(payload: &[u8], expected: u32) -> bool {
    compute_frame_crc32(payload) == expected
}

/// Hash domain separator for the explicit-null value marker.
pub const NULL_HASH_DOMAIN: &[u8] = b"rushdb:explicit-null:v1";

fn prefix64(bytes: &[u8; 32]) -> u64 {
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(prefix)
}

/// Stable 64-bit digest of arbitrary bytes, independent of process state.
#[must_use]
pub fn stable_hash64(bytes: &[u8]) -> u64 {

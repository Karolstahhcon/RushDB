//! Record-id hex decoding.

/// Decode exactly 16 bytes from a 32-character hex string (case-insensitive).
///
/// Returns `None` when the input is shorter than 32 characters or contains a
/// non-hex digit.
#[must_use]

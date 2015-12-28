use crate::error::TypesError;
use crate::value::Scalar;

fn u64be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

/// Order-preserving sign flip for signed integers.
fn int_encode(i: i64) -> [u8; 8] {
    u64be(u64::from_ne_bytes(i.to_ne_bytes()) ^ 0x8000_0000_0000_0000)
}

fn int_decode(b: &[u8]) -> i64 {
    let raw = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
    i64::from_ne_bytes((raw ^ 0x8000_0000_0000_0000).to_ne_bytes())
}

/// Total order for f64: positive floats have the sign bit flipped, negatives
/// are bit-inverted. -0.0 and +0.0 are distinct keys.
fn float_encode(f: f64) -> [u8; 8] {
    let bits = f.to_bits();
    let transformed = if bits & 0x8000_0000_0000_0000 == 0 {
        bits | 0x8000_0000_0000_0000
    } else {
        !bits
    };
    u64be(transformed)
}

fn float_decode(b: &[u8]) -> f64 {
    let raw = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
    let original = if raw & 0x8000_0000_0000_0000 == 0 {
        !raw
    } else {
        raw & !0x8000_0000_0000_0000
    };
    f64::from_bits(original)
}

/// Stable one-byte tag for a logical type used in encoded keys.
#[must_use]
pub fn logical_tag(ty: crate::model::LogicalType) -> u8 {
    use crate::model::LogicalType::{Boolean, DateTime, Float64, Int64, Utf8};
    match ty {
        Boolean => 1,
        Int64 => 2,
        Float64 => 3,
        Utf8 => 4,
        DateTime => 5,
    }

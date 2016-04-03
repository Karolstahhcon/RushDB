//! Bolt packed-stream value codec (encode/decode of the Neo4j binary value
//! types used by the Bolt protocol).

use serde_json::{Number, Value};

/// Marker for a tiny string of up to 15 bytes; the low nibble is the length.
pub const MARKER_TINY_STRING: u8 = 0x80;
/// Marker for a tiny list of up to 15 elements; the low nibble is the count.
pub const MARKER_TINY_LIST: u8 = 0x90;
/// Marker for a tiny map of up to 15 entries; the low nibble is the count.
pub const MARKER_TINY_MAP: u8 = 0xA0;
/// Marker for a tiny struct of up to 15 fields; the low nibble is the count.
pub const MARKER_TINY_STRUCT: u8 = 0xB0;
/// Marker for a null value.
pub const MARKER_NULL: u8 = 0xC0;
/// Marker for a 64-bit IEEE-754 float.
pub const MARKER_FLOAT64: u8 = 0xC1;
/// Marker for boolean `false`.
pub const MARKER_FALSE: u8 = 0xC2;
/// Marker for boolean `true`.
pub const MARKER_TRUE: u8 = 0xC3;
/// Marker for an 8-bit signed integer.
pub const MARKER_INT8: u8 = 0xC8;
/// Marker for a 16-bit signed integer.
pub const MARKER_INT16: u8 = 0xC9;
/// Marker for a 32-bit signed integer.
pub const MARKER_INT32: u8 = 0xCA;
/// Marker for a 64-bit signed integer.
pub const MARKER_INT64: u8 = 0xCB;
/// Marker for a string of up to 255 bytes.
pub const MARKER_STRING8: u8 = 0xD0;
/// Marker for a string of up to 65 535 bytes.
pub const MARKER_STRING16: u8 = 0xD1;
/// Marker for a string of up to 4 294 967 295 bytes.
pub const MARKER_STRING32: u8 = 0xD2;
/// Marker for a list of up to 255 elements.
pub const MARKER_LIST8: u8 = 0xD4;
/// Marker for a list of up to 65 535 elements.
pub const MARKER_LIST16: u8 = 0xD5;
/// Marker for a list of up to 4 294 967 295 elements.
pub const MARKER_LIST32: u8 = 0xD6;
/// Marker for a map of up to 255 entries.
pub const MARKER_MAP8: u8 = 0xD8;
/// Marker for a map of up to 65 535 entries.
pub const MARKER_MAP16: u8 = 0xD9;
/// Marker for a map of up to 4 294 967 295 entries.
pub const MARKER_MAP32: u8 = 0xDA;

/// Maximum number of values in one decoded container. This is independent of
/// the wire byte cap because a tiny payload can declare a huge list/map.
pub const MAX_CONTAINER_ITEMS: usize = 1_000_000;
/// Maximum nesting depth accepted from an untrusted Bolt message.
pub const MAX_NESTING_DEPTH: usize = 64;

/// A decoded Bolt message: a struct tag plus its already-decoded fields.
pub struct Struct {
    /// The Bolt struct tag (for example `0x01` for HELLO).
    pub tag: u8,
    /// The decoded fields, in wire order.
    pub fields: Vec<Value>,
}

// ---- encoding -------------------------------------------------------------

/// Encode a Bolt struct with `tag` and `fields` into `out`.
///
/// # Panics
/// Panics if `fields` exceeds the protocol's 15-field limit. Bolt defines only
/// a 4-bit tiny-struct marker; silently truncating the count would OR into the
/// marker bits and emit a corrupt, undecodable frame.
pub fn encode_struct(tag: u8, fields: Vec<Value>, out: &mut Vec<u8>) {
    let count = u8::try_from(fields.len())
        .ok()
        .filter(|count| *count <= 0x0F)
        .expect("Bolt structs carry at most 15 fields");
    out.push(MARKER_TINY_STRUCT | count);
    out.push(tag);
    for field in fields {
        encode_value(&field, out);
    }
}

/// Encode a JSON object as a Bolt map into `out`.
///
/// # Panics
/// Panics if the map exceeds the protocol's `u32` length limit.
#[expect(
    clippy::cast_possible_truncation,
    reason = "Bolt map markers store the entry count in one byte"
)]
pub fn encode_map(map: &serde_json::Map<String, Value>, out: &mut Vec<u8>) {
    if map.len() <= 0x0F {
        out.push(MARKER_TINY_MAP | map.len() as u8);
    } else if let Ok(len) = u8::try_from(map.len()) {
        out.push(MARKER_MAP8);
        out.push(len);
    } else if let Ok(len) = u16::try_from(map.len()) {
        out.push(MARKER_MAP16);
        out.extend_from_slice(&len.to_be_bytes());
    } else {
        out.push(MARKER_MAP32);
        out.extend_from_slice(
            &u32::try_from(map.len())
                .expect("Bolt map length exceeds the protocol's u32 limit")
                .to_be_bytes(),
        );
    }
    for (key, value) in map {
        encode_string(key, out);
        encode_value(value, out);
    }
}

/// Encode a JSON array as a Bolt list into `out`.
///
/// # Panics
/// Panics if the list exceeds the protocol's `u32` length limit.
#[expect(
    clippy::cast_possible_truncation,
    reason = "Bolt list markers store the element count in one byte"
)]
pub fn encode_list(list: &[Value], out: &mut Vec<u8>) {
    if list.len() <= 0x0F {
        out.push(MARKER_TINY_LIST | list.len() as u8);
    } else if let Ok(len) = u8::try_from(list.len()) {
        out.push(MARKER_LIST8);
        out.push(len);
    } else if let Ok(len) = u16::try_from(list.len()) {
        out.push(MARKER_LIST16);
        out.extend_from_slice(&len.to_be_bytes());
    } else {
        out.push(MARKER_LIST32);
        out.extend_from_slice(
            &u32::try_from(list.len())
                .expect("Bolt list length exceeds the protocol's u32 limit")
                .to_be_bytes(),
        );
    }
    for value in list {
        encode_value(value, out);
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "Bolt string markers store the byte length in 8 or 16 bits"
)]
fn encode_string(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len <= 0x0F {
        out.push(MARKER_TINY_STRING | len as u8);
    } else if let Ok(short) = u8::try_from(len) {
        out.push(MARKER_STRING8);
        out.push(short);
    } else if let Ok(short) = u16::try_from(len) {
        out.push(MARKER_STRING16);
        out.extend_from_slice(&short.to_be_bytes());
    } else {
        out.push(MARKER_STRING32);
        out.extend_from_slice(
            &u32::try_from(len)
                .expect("Bolt string length exceeds the protocol's u32 limit")
                .to_be_bytes(),
        );
    }
    out.extend_from_slice(bytes);
}

fn encode_int(i: i64, out: &mut Vec<u8>) {
    if (0..=127).contains(&i) {
        out.push(u8::try_from(i).expect("0..=127 fits in a u8"));
    } else if (-16..0).contains(&i) {
        out.push(u8::try_from(i & 0xFF).expect("masked value is 0..=255"));
    } else if let Ok(value) = i8::try_from(i) {
        out.push(MARKER_INT8);
        out.push(value.to_ne_bytes()[0]);
    } else if let Ok(value) = i16::try_from(i) {
        out.push(MARKER_INT16);
        out.extend_from_slice(&value.to_be_bytes());
    } else if let Ok(value) = i32::try_from(i) {
        out.push(MARKER_INT32);
        out.extend_from_slice(&value.to_be_bytes());
    } else {
        out.push(MARKER_INT64);

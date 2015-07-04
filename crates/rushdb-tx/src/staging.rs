//! Retained staging representation. Incoming decoded request objects and
//! commit-time decoding/planning are separate, still-unbudgeted allocations.
use rushdb_lmpg::{BatchOp, FieldChange};
use rushdb_storage::resources::{ByteBudget, ByteReservation};
use std::io::{self, Write};

const OP_METADATA_ALLOWANCE: usize = 256;
const MAX_JSON_DEPTH: usize = 64;

pub(crate) struct StagedOp {
    bytes: Vec<u8>,
    // Drop bytes before releasing their reservation.
    _reservation: ByteReservation,
}

struct Counter {
    bytes: usize,
    limit: usize,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let total = self
            .bytes
            .checked_add(bytes.len())
            .filter(|total| *total <= self.limit)
            .ok_or_else(|| {
                io::Error::other("resource exhausted: transaction staging byte limit")
            })?;
        self.bytes = total;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn validate_value(
    value: &serde_json::Value,
    depth: usize,
    remaining_nodes: &mut usize,
) -> Result<(), String> {
    if depth > MAX_JSON_DEPTH || *remaining_nodes == 0 {
        return Err("resource exhausted: transaction JSON depth/size limit".into());
    }
    *remaining_nodes -= 1;
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                validate_value(value, depth + 1, remaining_nodes)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                validate_value(value, depth + 1, remaining_nodes)?;
            }

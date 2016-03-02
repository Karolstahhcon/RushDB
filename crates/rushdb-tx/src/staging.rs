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

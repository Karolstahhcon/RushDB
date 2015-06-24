//! `rushdb_writer_hold_seconds{holder}`: how long each kind of work held the
//! storage backend's single writer. The store keeps the histogram
//! itself (atomic buckets per backend, `rushdb_storage::WriterHolds`); this
//! collector renders the snapshot taken at each scrape.

use prometheus::core::{Collector, Desc};
use prometheus::proto;

const NAME: &str = "rushdb_writer_hold_seconds";
const HELP: &str = "Time one write transaction held the storage writer, by holder";

pub(super) struct WriterHoldCollector {
    desc: Desc,
    snapshot: std::sync::Mutex<Vec<(rushdb_storage::WriterHolder, rushdb_storage::WriterHolds)>>,
}

impl WriterHoldCollector {
    pub(super) fn new() -> Self {
        Self {
            desc: Desc::new(

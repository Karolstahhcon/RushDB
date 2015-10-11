//! P00 crash-harness child: writes records across a configurable durability /
//! shard / concurrency mode, then aborts to simulate an unclean shutdown. The
//! parent tests (`crates/rushdb-storage-integration/tests/baseline.rs`)
//! spawn this binary and verify the durable prefix survives reopen + replay.
//!
//! Usage: `crash_child <db> <commits-per-writer> [--async] [--shards N]
//!         [--concurrent W] [--delay-ms M]`
//!
//! `--delay-ms M` (with `--concurrent W`) aborts after `M` ms without joining
//! the writers, so the crash lands mid-burst with commits in flight.

use std::sync::Arc;
use std::time::Duration;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::resources::{Durability, StorageResources};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

const TENANT: TenantId = TenantId::from_ordinal(66);

fn main() {
    let mut args = std::env::args().skip(1);
    let db = args.next().expect("usage: crash_child <db> <commits>");
    let commits: u64 = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8);

    let mut config = StorageResources::default();
    let mut concurrent = 1u64;
    let mut delay_ms = 0u64;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--async" => config.durability = Durability::Async,
            "--shards" => {
                config.shard_count = args.next().expect("--shards N").parse().unwrap();
                config.experimental_multi_shard = true;
            }
            "--concurrent" => concurrent = args.next().expect("--concurrent W").parse().unwrap(),
            "--delay-ms" => delay_ms = args.next().expect("--delay-ms M").parse().unwrap(),

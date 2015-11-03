//! JSONL import format tests: line-delimited ingestion must produce the
//! expected records. (CSV is decoded by the protocol's `ingest.csv`.)

use std::sync::Arc;

use rushdb_lmpg::import::import_jsonl;
use rushdb_lmpg::reconstruct::reconstruct_record;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;

fn setup() -> Lmpg {
    let store = Arc::new(Store::new_in_memory());
    Lmpg::new(store, TenantId::from_ordinal(71))
}

#[test]
fn jsonl_stream_imports_each_line() {
    let lmpg = setup();
    let bytes = b"{\"a\":1}\n\n{\"b\":\"x\",\"c\":null}\n";
    let refs = import_jsonl(&lmpg, "Row", &bytes[..]).unwrap();
    assert_eq!(refs.len(), 2);

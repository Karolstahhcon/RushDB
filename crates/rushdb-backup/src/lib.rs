//! Logical backup and restore for `RushDB`.
//!
//! This crate owns the JSONL backup surface only. It is **not** a migration
//! tool: migrations from other stores are performed outside the engine (see

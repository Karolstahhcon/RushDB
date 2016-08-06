//! A request's own bounds: the deadline, working-memory cap and
//! unindexed-walk limit a client may set. Each only tightens the server's
//! ceiling; the engine handle carries them into every read.

use rushdb_protocol::{ErrorCode, ProtocolError};

/// A request's own bounds: its deadline (the server's request

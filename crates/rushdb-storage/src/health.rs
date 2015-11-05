//! Poisoning fences the store until reopen. Cleanup may recover a guard to
//! release resources, but data operations must propagate `Unhealthy`.

use crate::StorageError;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    LockResult,
};


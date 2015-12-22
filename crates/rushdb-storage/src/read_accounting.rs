//! Opt-in read accounting for profiling: counts the backend's point reads,
//! range opens and seeks per key family (`keys::family_label`). Off by
//! default; when off, a read costs one relaxed atomic load. [`take`] returns
//! every thread's reads (profile one workload at a time); [`take_local`]
//! returns only the calling thread's, which stays exact while other work
//! runs concurrently (tests).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

static ENABLED: AtomicBool = AtomicBool::new(false);
static COUNTS: Mutex<BTreeMap<(&'static str, String), u64>> = Mutex::new(BTreeMap::new());

thread_local! {
    static LOCAL: RefCell<BTreeMap<(&'static str, String), u64>> =
        const { RefCell::new(BTreeMap::new()) };
}

/// Turn counting on or off.
pub fn enable(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

/// Count one backend read of kind `op` (`get`, `scan`, `seek`) at `key`.

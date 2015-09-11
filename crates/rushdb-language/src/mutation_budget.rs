//! Cumulative admission for a mutation's copy-on-write layer. Deleted or
//! replaced values are not refunded: originals, current versions, and the
//! eventual delta may coexist until publication.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use grafeo_common::types::Value;

use crate::LanguageError;

pub(crate) struct MutationBudget {

//! Regression: a JSON `null` scalar is a nullable occurrence, not a
//! mixed-type array. Previously `FieldChange::Set(json!(null))` was rejected
//! with `PlanError::MixedArray` (and, once a typed sibling existed, with a
//! `TypeMismatch { found: "?" }`), which made any record carrying an explicit
//! null property impossible to write and failed the whole SQL label.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::reader::try_access;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::json;

//! Lazy `DataFusion` schema providers for the SQL read path.
//!
//! The read path used to reconstruct every label's records (schema inference)
//! and materialize every relationship table on every query, even `SELECT 1`.
//! These providers defer that work to the first time a table is actually
//! resolved, so a query pays only for the labels/edge types it references.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use rushdb_lmpg::Lmpg;
use rushdb_storage::ReadView;
use rushdb_types::ids::TenantId;

use crate::edge_provider::EdgeTable;
use crate::scan_provider::LabelTable;
use crate::{relationship_types_for_tenant, SqlError};
use rushdb_lmpg::CatalogAccess;

fn df_err(error: &SqlError) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

/// Lazily builds one label table per referenced label.
pub(crate) struct LazyLabelSchema {
    /// The tenant's facade for the index planner (store and tenant only).
    lmpg: Arc<Lmpg>,
    snapshot: Arc<ReadView>,
    names: Vec<String>,
    visible: CatalogAccess,
    cache: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl fmt::Debug for LazyLabelSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyLabelSchema")
            .field("labels", &self.names)
            .finish_non_exhaustive()
    }
}

impl LazyLabelSchema {
    /// Enumerate the tenant's labels (a catalog read, not a record scan).
    ///
    /// # Errors
    /// Returns a storage error when the label catalog cannot be read.
    /// Under an enforced policy only its allowed labels are listed.
    pub(crate) fn new(
        lmpg: &Lmpg,
        snapshot: Arc<ReadView>,
        visible: CatalogAccess,
    ) -> Result<Self, SqlError> {
        let mut names = rushdb_lmpg::reader::try_access::labels(&snapshot, lmpg.tenant)?;
        names.retain(|label| visible.label(label));
        Ok(Self {
            // The request's handle: its bounds reach the scans.
            lmpg: Arc::new(lmpg.handle()),
            snapshot,
            names,
            visible,
            cache: Mutex::new(HashMap::new()),
        })
    }
}

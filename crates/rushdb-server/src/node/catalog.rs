//! Node catalog operations. Context entry points retain caller-owned
//! snapshot, tenant, cancellation, and budget semantics.

use super::working_set::{value_bytes, WorkingSet};
use super::{
    access, logical_type_name, paginate_entries, type_name_str, Arc, CachedMetadata, OwnerKind,
    PropertyMetadata, RequestScope, ValueDomainIndex,
};
use rushdb_lmpg::reader::TryRead;
use rushdb_types::value_match::ValueMatcher;

/// A text property's exact distinct count and, when few, its values with owner counts.
type ScannedText = (u64, Vec<(serde_json::Value, u64)>);
/// Records one listing of long text values may read
/// (`long_text_values_in_view`) before it leaves the values out.
const LONG_VALUE_WALK: u64 = 65_536;
impl RequestScope<'_> {
    // The tenant identity is collision-free and derived from the request
    // scope, so the keyspace prefix is the sole isolation boundary. Records
    // never carry a discriminator property; nothing is stamped on write,
    // injected into queries, or stripped from responses. The scope value is
    // only used to derive the tenant id and to match the protocol context.

    // ---- graph ----------------------------------------------------------

    /// Sorted labels present for the current tenant.
    ///
    /// # Errors
    ///
    /// Returns a message when the label catalog cannot be read.
    pub fn labels(&self) -> Result<Vec<String>, String> {
        Ok(self.metadata_cached()?.0.clone())
    }

    /// Read labels from a caller-owned snapshot. Protocol describe paths use
    /// this entry point so catalog metadata cannot switch to a newer commit
    /// after the request context has pinned its view.
    pub(crate) fn labels_in_view(
        &self,
        view: &rushdb_storage::ReadView,
    ) -> Result<Vec<String>, String> {
        let data = self.metadata_cached_in_view(view)?;
        Ok(data.0.clone())
    }

    /// Read exact per-label counts from a caller-owned snapshot.
    pub(crate) fn label_counts_in_view(
        &self,
        view: &rushdb_storage::ReadView,
    ) -> Result<Vec<(String, u64)>, String> {
        let data = self.metadata_cached_in_view(view)?;
        Ok(data.1.clone())
    }

    /// Bounded, commit-index-keyed cache of `(labels, per-label counts)`.
    /// Results are exact for the current commit index; a later commit misses.
    pub(super) fn metadata_cached(&self) -> Result<CachedMetadata, String> {
        let view = self.store.read_view().map_err(|e| e.to_string())?;
        self.metadata_cached_in_view(&view)
    }

    pub(super) fn metadata_cached_in_view(
        &self,
        view: &rushdb_storage::ReadView,
    ) -> Result<CachedMetadata, String> {
        let tenant = self.tenant();
        let ci = view.commit_index();
        let policy_generation = rushdb_authz::load_active_policy_in_view(view, tenant)
            .map_err(|e| e.to_string())?
            .map_or(0, |active| active.generation);
        let cache_key = (tenant, policy_generation);
        {
            let mut cache = self
                .metadata_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(cache) = cache.as_mut() {
                if let Some((cached_ci, data)) = cache.get(&cache_key) {
                    if *cached_ci == ci {
                        return Ok(data.clone());
                    }
                }
            }
        }
        let labels =
            rushdb_lmpg::reader::try_access::labels(view, tenant).map_err(|e| e.to_string())?;
        let counts = rushdb_lmpg::reader::try_access::label_counts(view, tenant)
            .map_err(|e| e.to_string())?;
        let data = std::sync::Arc::new((labels, counts));
        let mut cache = self
            .metadata_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(cache) = cache.as_mut() {
            let bytes = data
                .0
                .iter()
                .map(|label| label.len() + 16)
                .chain(data.1.iter().map(|(label, _)| label.len() + 24))
                .sum::<usize>()
                + 64;
            cache.insert(cache_key, (ci, data.clone()), bytes);
        }
        Ok(data)
    }

    /// Per-label live record counts from the maintained label catalog
    /// (`O(labels)`), used by unscoped schema description.
    ///
    /// # Errors
    ///
    /// Returns a message when the label catalog cannot be read.
    pub fn label_counts(&self) -> Result<Vec<(String, u64)>, String> {
        Ok(self.metadata_cached()?.1.clone())
    }

    /// Typed scalar definitions `(type, pid)` of an unscoped record property,
    /// or `None` when no record ever carried the name.
    /// The property's scalar definitions of `types` (all when empty).
    fn scalar_definitions_of(
        &self,
        view: &rushdb_storage::ReadView,
        property: &str,
        types: &[rushdb_types::model::LogicalType],
    ) -> Result<Option<Vec<(rushdb_types::model::LogicalType, u64)>>, String> {
        Ok(self
            .scalar_definitions(view, property)?
            .map(|mut definitions| {
                if !types.is_empty() {
                    definitions.retain(|(ty, _)| types.contains(ty));
                }
                definitions
            }))
    }

    fn scalar_definitions(
        &self,
        view: &rushdb_storage::ReadView,
        property: &str,
    ) -> Result<Option<Vec<(rushdb_types::model::LogicalType, u64)>>, String> {
        use rushdb_lmpg::reader::keys_ext as kx;
        use rushdb_types::model::{LogicalType, TypeState};
        let t = self.tenant();
        let norm = rushdb_types::value::normalize_name(property);
        let sig = kx::family_sig_key(t, "default", OwnerKind::Record, &norm);
        let Some(raw) = view.get(&sig).map_err(|e| e.to_string())? else {
            return Ok(None);
        };
        let fid = u64::from_be_bytes(
            raw.as_slice()
                .try_into()
                .map_err(|_| "corrupt family signature".to_string())?,
        );
        let mut out = Vec::new();
        for ty in [
            LogicalType::Int64,
            LogicalType::Float64,
            LogicalType::Utf8,
            LogicalType::Boolean,
            LogicalType::DateTime,
        ] {
            let Some(pid_raw) = view
                .get(&kx::definition_key(t, fid, TypeState::Known(ty)))
                .map_err(|e| e.to_string())?
            else {
                continue;
            };
            out.push((
                ty,
                u64::from_be_bytes(
                    pid_raw
                        .as_slice()
                        .try_into()
                        .map_err(|_| "corrupt property definition".to_string())?,
                ),
            ));
        }
        Ok(Some(out))
    }

    /// Whether any record under `labels` (all labels when empty) holds an
    /// explicit null for `property`, under any of its definitions, the
    /// untyped one (a null seen before any value) included. Nulls are not
    /// dictionary entries, so a list read from the dictionary would miss them.
    ///
    /// # Errors
    ///
    /// Returns a message when a read fails or a row is corrupt.
    pub(crate) fn explicit_nulls_in_view(
        &self,
        view: &rushdb_storage::ReadView,
        property: &str,
        labels: &[String],
    ) -> Result<bool, String> {
        use rushdb_lmpg::reader::keys_ext as kx;
        use rushdb_types::model::{LogicalType, TypeState};
        let t = self.tenant();
        let norm = rushdb_types::value::normalize_name(property);
        let sig = kx::family_sig_key(t, "default", OwnerKind::Record, &norm);
        let Some(raw) = view.get(&sig).map_err(|e| e.to_string())? else {
            return Ok(false);
        };
        let fid = u64::from_be_bytes(
            raw.as_slice()
                .try_into()
                .map_err(|_| "corrupt family signature".to_string())?,
        );
        let states = [
            TypeState::Unresolved,
            TypeState::Known(LogicalType::Int64),
            TypeState::Known(LogicalType::Float64),
            TypeState::Known(LogicalType::Utf8),
            TypeState::Known(LogicalType::Boolean),
            TypeState::Known(LogicalType::DateTime),
        ];
        for state in states {
            let Some(pid_raw) = view
                .get(&kx::definition_key(t, fid, state))
                .map_err(|e| e.to_string())?
            else {
                continue;
            };
            let pid = u64::from_be_bytes(
                pid_raw
                    .as_slice()
                    .try_into()
                    .map_err(|_| "corrupt property definition".to_string())?,
            );
            if rushdb_lmpg::derived::has_nulls(view, t, OwnerKind::Record, pid, labels)
                .map_err(|e| e.to_string())?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether the value dictionary alone answers `pid` exactly under
    /// `labels` (all labels when empty): no array occurrence (whole-array
    /// semantics differ from element values) and no value too long for the
    /// dictionary.
    fn dictionary_answers(
        &self,
        view: &rushdb_storage::ReadView,
        pid: u64,
        labels: &[String],
    ) -> Result<bool, String> {
        let t = self.tenant();
        let (_, array_labels) =
            rushdb_lmpg::derived::array_markers(view, t, OwnerKind::Record, pid)
                .map_err(|e| e.to_string())?;
        let has_array = array_labels
            .iter()
            .any(|label| labels.is_empty() || labels.contains(label));
        let long = rushdb_lmpg::derived::long_values(view, t, OwnerKind::Record, pid, labels)
            .map_err(|e| e.to_string())?;
        Ok(!has_array && long == 0)
    }

    /// O(1)-per-definition exact distinct count for an unscoped property from
    /// the on-disk counters, or `None` (caller scans) for an
    /// array property or a property with no scalar definition.
    pub(crate) fn distinct_count_from_index_in_view(
        &self,
        view: &rushdb_storage::ReadView,
        property: &str,
    ) -> Result<Option<u64>, String> {
        let Some(definitions) = self.scalar_definitions(view, property)? else {
            return Ok(None);
        };
        let mut total = 0u64;
        for (_, pid) in &definitions {
            let (has_array, _) =
                rushdb_lmpg::derived::array_markers(view, self.tenant(), OwnerKind::Record, *pid)
                    .map_err(|e| e.to_string())?;
            if has_array {
                return Ok(None);
            }
            total = total.saturating_add(
                rushdb_lmpg::derived::distinct_count(view, self.tenant(), OwnerKind::Record, *pid)
                    .map_err(|e| e.to_string())?,
            );
        }
        Ok((!definitions.is_empty()).then_some(total))
    }

    /// Value domain of a property under `labels` (all labels when empty) from
    /// the value dictionary: exact
    /// distinct count, explicit-null presence, and numeric min/max. `None`
    /// (caller scans) when a definition holds arrays or long values, or no
    /// scalar definition exists. `numeric_only` tells the caller whether
    /// `min`/`max` are meaningful. The distinct set is working set while it
    /// is counted (`meter`), released before returning.
    ///
    /// # Errors
    ///
    /// Returns a message when a read fails, a row is corrupt, or the meter
    /// stops the read.
    pub(crate) fn value_domain_from_index_in_view(
        &self,
        view: &rushdb_storage::ReadView,
        property: &str,
        labels: &[String],
        meter: &mut WorkingSet<'_>,
    ) -> Result<Option<ValueDomainIndex>, String> {
        self.value_domain_of_types_in_view(view, property, labels, &[], meter)
    }

    /// [`Self::value_domain_from_index_in_view`] over the property's
    /// definitions of `types` only (all when empty): one entry of a type
    /// split reads its own values, not the whole property's.
    #[expect(
        clippy::too_many_lines,
        reason = "one linear catalog scan; splitting would scatter the shared read-view borrows"
    )]
    pub(crate) fn value_domain_of_types_in_view(
        &self,
        view: &rushdb_storage::ReadView,
        property: &str,
        labels: &[String],
        types: &[rushdb_types::model::LogicalType],
        meter: &mut WorkingSet<'_>,
    ) -> Result<Option<ValueDomainIndex>, String> {
        use rushdb_types::model::LogicalType;
        use rushdb_types::value::{scalar_cmp, scalar_to_json, Scalar};
        use std::cmp::Ordering;
        let Some(definitions) = self.scalar_definitions_of(view, property, types)? else {
            return Ok(None);
        };
        if definitions.is_empty() {
            return Ok(None);
        }
        let t = self.tenant();
        if let [label] = labels {
            if let Some(domain) = self.label_domain_in_view(view, label, &definitions)? {
                return Ok(Some(domain));
            }
        }
        let mut distinct = std::collections::BTreeSet::<Vec<u8>>::new();

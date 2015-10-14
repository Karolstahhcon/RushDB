//! Vector-index metadata reads: existence and raw rows for write-path alias
//! resolution, and the readiness/coverage statistics served by `describe`.
//! Statistics come from the derived catalog and binding rows, never from a
//! reconstruction of every record (C9, cloud-capacity F10).

use super::{access, Arc, RequestScope};

impl RequestScope<'_> {
    /// Exact number of `label` records carrying record property `property`,
    /// from the derived support catalog (summed over the property family's
    /// typed definitions).
    fn property_support_in_label(
        &self,
        snap: &rushdb_storage::ReadView,
        label: &str,
        property: &str,
    ) -> Result<u64, String> {
        let tenant = self.tenant();
        let sig = rushdb_lmpg::reader::keys_ext::family_sig_key(
            tenant,
            "default",
            rushdb_types::ids::OwnerKind::Record,
            &rushdb_types::value::normalize_name(property),
        );
        let Some(raw) = snap.get(&sig).map_err(|e| e.to_string())? else {
            return Ok(0);
        };
        let family = raw
            .get(..8)
            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
            .map(u64::from_be_bytes)
            .ok_or("corrupt property family signature")?;
        let mut total = 0u64;
        for definition in
            access::definitions_of_family(snap, tenant, family).map_err(|e| e.to_string())?
        {
            total = total.saturating_add(
                rushdb_lmpg::derived::prop_support_label(
                    snap,
                    tenant,
                    rushdb_types::ids::OwnerKind::Record,
                    definition.property_id,
                    label,
                )
                .map_err(|e| e.to_string())?,
            );
        }
        Ok(total)
    }

    /// Raw vector-index metadata rows (no readiness stats): the cheap form

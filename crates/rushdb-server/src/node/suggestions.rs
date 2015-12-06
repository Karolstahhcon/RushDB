//! Catalog reads over names and the exact value dictionaries: names spelled
//! like a requested one, a compact schema in one read, and
//! value overlap between property pairs.
//!
//! Value overlap is evidence only: how much of one property's distinct
//! values another holds, counted over the whole dictionaries rather than
//! samples. Which pairs are references is decided by the caller (the Core
//! platform's relationship suggestions), not here.

use super::working_set::{value_bytes, WorkingSet};
use super::{Arc, RequestScope};
use rushdb_lmpg::CatalogAccess;
use rushdb_protocol::{
    JoinEnd, JoinPair, LabelSummary, NameSuggestion, PropertyDescription, PropertySummary,
    RelationshipSummary, ValueOverlap,
};
use rushdb_types::names::name_similarity;
use std::collections::{BTreeMap, HashMap};

/// Most distinct values compared per property; a larger dictionary is
/// reported as not compared.
const MAX_COMPARED_DISTINCT: u64 = 200_000;
/// Most pairs one `describe.valueOverlap` compares.
pub(crate) const MAX_OVERLAP_PAIRS: usize = 10_000;

/// A value's join key: strings as stored, numbers and booleans as text, so a
/// numeric id matches the same id stored as a string.
fn join_key(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => Some(text.clone()),
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

/// Facts about one (label, property) value overlap reads.
struct Column {
    label: String,
    property: String,
    records_count: u64,
    is_array: bool,
}

/// A property's distinct values with owner counts, or why they were not read.
enum Dictionary {
    Values(Values),
    NotCompared(&'static str),
}

/// Distinct join keys with owner counts, and the numeric span when (nearly)
/// every key is a number, as digits stored in strings often are.
struct Values {
    counts: HashMap<String, u64>,
    span: Option<(f64, f64)>,
}

impl Values {
    fn new(counts: HashMap<String, u64>) -> Self {
        let numbers: Vec<f64> = counts.keys().filter_map(|key| key.parse().ok()).collect();
        #[expect(
            clippy::cast_precision_loss,
            reason = "a share of counts; precision is irrelevant"
        )]
        let numeric = !numbers.is_empty() && numbers.len() as f64 >= 0.9 * counts.len() as f64;
        let span = numeric.then(|| {
            numbers
                .iter()
                .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), &n| {
                    (lo.min(n), hi.max(n))
                })
        });
        Self { counts, span }
    }

    fn len(&self) -> usize {
        self.counts.len()
    }
}

impl RequestScope<'_> {
    /// Every (label, property) of the catalog `access` lists, from catalog
    /// indexes only.
    pub(crate) fn catalog_columns(
        &self,
        view: &Arc<rushdb_storage::ReadView>,
        labels: &[String],
        access: &CatalogAccess,
    ) -> Result<Vec<PropertyDescription>, String> {
        let body = serde_json::json!({ "labels": labels, "detail": "catalog" });
        let mut columns = self.describe_properties_in_view(view, &body, None, &|| false)?;
        columns.retain(|column| {
            column.label.as_deref().is_some_and(|label| {
                access.column_listed(label, &column.name, column.records_count)
            })
        });
        Ok(columns)
    }

    /// Labels and properties spelled like `text`, best first, with a
    /// score of at least 0.5. `Label.property` text scores both parts.
    pub(crate) fn suggest_names_in_view(
        &self,
        view: &Arc<rushdb_storage::ReadView>,
        text: &str,
        limit: usize,
        access: &CatalogAccess,
    ) -> Result<Vec<NameSuggestion>, String> {
        let (label_part, property_part) = match text.split_once('.') {
            Some((label, property)) => (Some(label), property),
            None => (None, text),
        };
        let mut out: Vec<NameSuggestion> = Vec::new();
        if label_part.is_none() {
            for (label, count) in self.label_counts_in_view(view)? {
                if count == 0 || !access.label_listed(&label, count) {
                    continue;
                }
                let score = name_similarity(text, &label);
                out.push(NameSuggestion {
                    label,
                    property: None,
                    score,
                });
            }
        }
        for column in self.catalog_columns(view, &[], access)? {
            let Some(label) = column.label else { continue };
            let property_score = name_similarity(property_part, &column.name);
            let score = match label_part {
                Some(wanted) => property_score.min(name_similarity(wanted, &label)),
                None => property_score,
            };
            out.push(NameSuggestion {
                label,
                property: Some(column.name),
                score,
            });
        }
        out.retain(|suggestion| suggestion.score >= 0.5);
        // Equal scores (similarity folds case and plurals) rank the name
        // written exactly first, then one equal but for case, then a label
        // before a property of the same spelling.
        let closeness = |suggestion: &NameSuggestion| {
            let (wanted, name) = match (&suggestion.property, label_part) {
                (Some(property), _) => (property_part, property.as_str()),
                (None, _) => (text, suggestion.label.as_str()),
            };
            let label_ok = label_part.is_none_or(|wanted| wanted == suggestion.label);
            let spelled = if wanted == name && label_ok {
                0
            } else if wanted.eq_ignore_ascii_case(name) {
                1
            } else {
                2
            };
            (spelled, u8::from(suggestion.property.is_some()))
        };
        out.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| closeness(a).cmp(&closeness(b)))
                .then_with(|| a.label.cmp(&b.label))
                .then_with(|| a.property.cmp(&b.property))
        });
        out.dedup_by(|a, b| a.label == b.label && a.property == b.property);
        out.truncate(limit);
        Ok(out)
    }

    /// Every label with its properties in one read. Each property's
    /// dictionary is working set (`meter`) only while it is summarized.
    pub(crate) fn schema_summary_in_view(
        &self,
        view: &Arc<rushdb_storage::ReadView>,
        labels: &[String],
        few_values: u64,
        access: &CatalogAccess,
        meter: &mut WorkingSet<'_>,
    ) -> Result<Vec<LabelSummary>, String> {
        let counts: BTreeMap<String, u64> = self
            .label_counts_in_view(view)?
            .into_iter()
            .filter(|(label, count)| {
                *count > 0
                    && (labels.is_empty() || labels.contains(label))
                    && access.label_listed(label, *count)
            })
            .collect();
        let mut properties: BTreeMap<String, Vec<PropertySummary>> = BTreeMap::new();
        for column in self.catalog_columns(view, labels, access)? {
            let Some(label) = column.label.clone() else {
                continue;
            };
            if !counts.contains_key(&label) {
                continue;
            }
            meter.check()?;
            let summary =
                self.property_summary(view, &label, &column, few_values, access, meter)?;
            properties.entry(label).or_default().push(summary);
        }
        let mut shapes = self.describe_relationships_in_view(view)?;
        shapes.retain(|shape| {
            access.shape_listed(
                &shape.relationship_type,
                &shape.source_label,
                &shape.target_label,
                shape.count,
            )
        });
        Ok(counts
            .into_iter()
            .map(|(name, records_count)| {
                let mut relationships: Vec<RelationshipSummary> = Vec::new();
                for shape in &shapes {
                    if shape.source_label == name {
                        relationships.push(RelationshipSummary {
                            relationship_type: shape.relationship_type.clone(),
                            label: shape.target_label.clone(),
                            direction: "out".into(),
                            count: shape.count,
                        });
                    }
                    if shape.target_label == name && shape.source_label != name {
                        relationships.push(RelationshipSummary {
                            relationship_type: shape.relationship_type.clone(),
                            label: shape.source_label.clone(),
                            direction: "in".into(),
                            count: shape.count,
                        });
                    }
                }
                let mut properties = properties.remove(&name).unwrap_or_default();
                properties.sort_by(|a, b| a.name.cmp(&b.name));
                LabelSummary {
                    name,
                    records_count,
                    properties,
                    relationships,
                }
            })
            .collect())
    }

    fn property_summary(
        &self,
        view: &Arc<rushdb_storage::ReadView>,
        label: &str,
        column: &PropertyDescription,

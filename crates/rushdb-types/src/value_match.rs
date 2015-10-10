//! Matching a search text against stored values: `contains`,
//! `prefix`, `exact` and `similar` (near spellings), and dates written in
//! another layout. The wire form is `rushdb_protocol::ValueSearch`; this is
//! the engine's side of it, shared by the value dictionaries, the facet
//! scan and the cross-property value search.
//!
//! A string matches as stored; a number, boolean or datetime matches as the
//! records response renders it; an array matches when any element does. A
//! text that reads as a date (year first, at least a month: `1993-11-12`,
//! `1993/11/12`, `2010/2/22`, `2013-06`) also matches values holding that
//! date in another layout (`1993-11-12T00:00:00Z`, `1993.11.12`,
//! `19931112`, `201306`), at the text's precision.

use std::borrow::Cow;
use std::collections::BTreeSet;

use serde_json::Value as Json;

/// How the search text must match a value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TextMatch {
    /// The value contains the text.
    #[default]
    Contains,
    /// The value starts with the text.
    Prefix,
    /// The value equals the text.
    Exact,
    /// The value is spelled like the text (see [`similarity`]).
    Similar,
}

/// A search text prepared for matching many values.
#[derive(Debug, Clone)]
pub struct ValueMatcher {
    text: String,
    needle: String,
    matching: TextMatch,
    case_sensitive: bool,
    min_similarity: f64,
    date: Option<DateText>,
}

impl ValueMatcher {
    /// The least similarity a `similar` match needs when none is given.
    pub const DEFAULT_MIN_SIMILARITY: f64 = 0.6;

    /// Prepares `text` for matching (case-insensitive unless
    /// `case_sensitive`); `min_similarity` applies to `similar` only.
    #[must_use]
    pub fn new(
        text: &str,
        matching: TextMatch,
        case_sensitive: bool,
        min_similarity: Option<f64>,
    ) -> Self {
        Self {
            text: text.to_string(),
            needle: if case_sensitive {
                text.to_string()
            } else {
                text.to_lowercase()
            },
            matching,

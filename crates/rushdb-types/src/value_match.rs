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
            case_sensitive,
            min_similarity: min_similarity.unwrap_or(Self::DEFAULT_MIN_SIMILARITY),
            date: parse_date_text(text, false),
        }
    }

    /// How the text must match.
    #[must_use]
    pub fn matching(&self) -> TextMatch {
        self.matching
    }

    /// Whether `value` matches.
    #[must_use]
    pub fn matches(&self, value: &Json) -> bool {
        let text = match value {
            Json::String(text) => Cow::Borrowed(text.as_str()),
            Json::Number(number) => Cow::Owned(number.to_string()),
            Json::Bool(flag) => Cow::Owned(flag.to_string()),
            Json::Array(items) => return items.iter().any(|item| self.matches(item)),
            Json::Null | Json::Object(_) => return false,
        };
        let text = if self.case_sensitive {
            text
        } else {
            Cow::Owned(text.to_lowercase())
        };
        let textual = match self.matching {
            TextMatch::Contains => text.contains(self.needle.as_str()),
            TextMatch::Prefix => text.starts_with(self.needle.as_str()),
            TextMatch::Exact => *text == *self.needle,
            TextMatch::Similar => similarity(&self.text, &text) >= self.min_similarity,
        };
        textual
            || self.date.is_some_and(|wanted| {
                parse_date_text(&text, true)
                    .is_some_and(|held| wanted.matches(held, self.matching == TextMatch::Exact))
            })
    }

    /// How close `value` is to the text, from 0 to 1 (see [`similarity`]);
    /// arrays score their closest element.
    #[must_use]
    pub fn similarity(&self, value: &Json) -> f64 {
        match value {
            Json::String(text) => similarity(&self.text, text),
            Json::Number(number) => similarity(&self.text, &number.to_string()),
            Json::Bool(flag) => similarity(&self.text, &flag.to_string()),
            Json::Array(items) => items
                .iter()
                .map(|item| self.similarity(item))
                .fold(0.0, f64::max),
            Json::Null | Json::Object(_) => 0.0,
        }
    }
}

/// Lowercase alphanumeric words of `text`; everything else separates them.
fn similarity_words(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_owned)
        .collect()
}

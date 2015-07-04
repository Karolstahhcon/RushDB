//! Names of labels and properties compared the way people misspell them:
//! tokens split on case and separators with plural endings dropped, then a
//! spelling score. Shared by the catalog's name suggestions and SQL's
//! "did you mean" errors.

use std::collections::BTreeSet;

/// Lowercase name tokens: camelCase and every non-alphanumeric character
/// split words, and a plural ending is dropped (`ies` -> `y`; `es` after
/// o, s, x, z, ch, sh; else a final `s`), as the Core platform's
/// relationship analysis does (it drops `ies`/`s` only).
#[must_use]
pub fn name_tokens(name: &str) -> Vec<String> {
    let mut words: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut previous_lower = false;
    for c in name.chars() {
        if !c.is_alphanumeric() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            previous_lower = false;
            continue;
        }
        if c.is_uppercase() && previous_lower && !current.is_empty() {
            words.push(std::mem::take(&mut current));
        }
        previous_lower = c.is_lowercase() || c.is_ascii_digit();
        current.extend(c.to_lowercase());
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
        .into_iter()
        .map(|word| {
            if let Some(stem) = word.strip_suffix("ies") {
                format!("{stem}y")

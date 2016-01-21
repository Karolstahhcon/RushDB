//! Canonical Date/Datetime handling backed by Chrono's ISO-8601 calendar
//! implementation. `RushDB` stores instants as UTC epoch milliseconds.

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};

const DATE: &str = "%Y-%m-%d";
const LOCAL_DATETIME: &str = "%Y-%m-%dT%H:%M:%S%.f";
const MS_PER_DAY: i64 = 86_400_000;

/// Parse a date-only value, RFC 3339 timestamp, or timezone-less ISO local
/// datetime (defined by `RushDB` as UTC) into epoch milliseconds.
#[must_use]
pub fn parse_datetime(value: &str) -> Option<i64> {
    let value = value.trim();
    // A signed year (`-0032-01-01T08:00:00Z`, ISO 8601 astronomical year
    // numbering: year 0 is 1 BC) in the same layouts as an unsigned one.
    // RFC 3339 has no signed years, so Chrono's RFC 3339 parser rejects
    // them; without this a property of BC and AD dates split by sign into
    // text and datetimes.
    if let Some(ms) = parse_signed_year(value) {
        return Some(ms);
    }
    if let Ok(datetime) = DateTime::parse_from_rfc3339(value) {
        return Some(datetime.timestamp_millis());
    }

    // Chrono follows RFC 3339's uppercase `Z`; accepting lowercase `z`
    // preserves the database's historical input convenience at the edge.
    if let Some(prefix) = value.strip_suffix('z') {
        let normalized = format!("{prefix}Z");
        if let Ok(datetime) = DateTime::parse_from_rfc3339(&normalized) {
            return Some(datetime.timestamp_millis());
        }
    }

    if let Ok(datetime) = NaiveDateTime::parse_from_str(value, LOCAL_DATETIME) {
        return Some(datetime.and_utc().timestamp_millis());
    }
    NaiveDate::parse_from_str(value, DATE)
        .ok()?
        .and_hms_opt(0, 0, 0)
        .map(|datetime| datetime.and_utc().timestamp_millis())
}

/// [`parse_datetime`] for a `-` or `+` signed year: date-only, local
/// datetime with seconds, or with `Z` or a `±HH:MM` offset; `T` separates
/// date and time.
fn parse_signed_year(value: &str) -> Option<i64> {
    let (sign, rest) = value.split_at_checked(1)?;
    let digits = rest.split('-').next()?;
    if !matches!(sign, "-" | "+") || digits.len() < 4 || !digits.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    if let Ok(datetime) = DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f%:z") {
        return Some(datetime.timestamp_millis());
    }
    if let Some(local) = value.strip_suffix(['Z', 'z']) {
        return NaiveDateTime::parse_from_str(local, LOCAL_DATETIME)
            .ok()
            .map(|datetime| datetime.and_utc().timestamp_millis());
    }
    if let Ok(datetime) = NaiveDateTime::parse_from_str(value, LOCAL_DATETIME) {
        return Some(datetime.and_utc().timestamp_millis());
    }
    NaiveDate::parse_from_str(value, DATE)
        .ok()?
        .and_hms_opt(0, 0, 0)
        .map(|datetime| datetime.and_utc().timestamp_millis())
}

/// Convert calendar components to epoch milliseconds, rejecting values that
/// fall outside the representable range.
///
/// Returns `None` if any component is out of range or the combination is not a

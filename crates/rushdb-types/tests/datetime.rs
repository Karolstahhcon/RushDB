//! Canonical Date/Datetime parse + serialize round-trips (the engine's typed
//! datetime value layer).

use rushdb_types::datetime::{ms_to_datetime_string, parse_datetime};

#[test]
fn date_only_parses_to_midnight_utc_and_roundtrips() {
    let ms = parse_datetime("2026-07-23").expect("date-only");
    assert_eq!(ms % 86_400_000, 0, "midnight UTC");
    assert_eq!(ms_to_datetime_string(ms), "2026-07-23");
}

#[test]
fn full_iso_with_time_roundtrips() {
    let ms = parse_datetime("2026-07-23T10:30:00Z").expect("iso");
    assert_eq!(ms_to_datetime_string(ms), "2026-07-23T10:30:00.000Z");

    let ms = parse_datetime("2026-07-23T10:30:00.123Z").expect("iso ms");
    assert_eq!(ms_to_datetime_string(ms), "2026-07-23T10:30:00.123Z");
}

#[test]
fn timezone_offsets_apply() {
    // 10:30+02:00 == 08:30Z
    let utc = parse_datetime("2026-07-23T08:30:00Z").unwrap();
    let offset = parse_datetime("2026-07-23T10:30:00+02:00").unwrap();
    assert_eq!(utc, offset);

    let neg = parse_datetime("2026-07-23T06:30:00-02:00").unwrap();
    assert_eq!(utc, neg);
}

#[test]
fn invalid_dates_rejected() {
    assert!(parse_datetime("2026-02-30").is_none(), "Feb 30");
    assert!(parse_datetime("2026-13-01").is_none(), "month 13");
    assert!(parse_datetime("2026-00-10").is_none(), "month 0");
    assert!(parse_datetime("2026-07-32").is_none(), "day 32");
    assert!(parse_datetime("2026-07-23T25:00:00Z").is_none(), "hour 25");
    assert!(parse_datetime("not-a-date").is_none());

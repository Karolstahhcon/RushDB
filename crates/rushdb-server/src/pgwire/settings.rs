//! Session settings that bound a client's own statements.
//!
//! The names are PostgreSQL's and `CockroachDB`'s, so drivers and tools that
//! already set them work unchanged: `statement_timeout`, `work_mem` (here
//! the working-memory cap of the whole statement), and `CockroachDB`'s
//! `disallow_full_table_scans` with `large_full_scan_rows`. Each may only
//! tighten the server's ceilings. A value is set with `SET`, cleared with
//! `RESET` or `SET … = DEFAULT`, read with `SHOW`, or passed as a startup
//! parameter; it lives in the connection's metadata.

use std::collections::HashMap;

use pgwire::error::PgWireResult;
use serde_json::{Map, Value};

use super::err;

/// One setting this module answers for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Setting {
    StatementTimeout,
    WorkMem,
    DisallowFullTableScans,
    LargeFullScanRows,
}

const SETTINGS: [Setting; 4] = [
    Setting::StatementTimeout,
    Setting::WorkMem,
    Setting::DisallowFullTableScans,
    Setting::LargeFullScanRows,
];

impl Setting {
    pub(super) fn name(self) -> &'static str {
        match self {
            Setting::StatementTimeout => "statement_timeout",
            Setting::WorkMem => "work_mem",
            Setting::DisallowFullTableScans => "disallow_full_table_scans",
            Setting::LargeFullScanRows => "large_full_scan_rows",
        }
    }

    fn of(name: &str) -> Option<Self> {
        SETTINGS
            .into_iter()
            .find(|setting| setting.name().eq_ignore_ascii_case(name))
    }

    /// What `SHOW` answers when the session has not set it: no bound of
    /// its own (the server's ceilings still apply).
    fn unset(self) -> &'static str {
        match self {
            Setting::StatementTimeout | Setting::LargeFullScanRows => "0",
            Setting::WorkMem => "server",
            Setting::DisallowFullTableScans => "off",
        }
    }

    /// The canonical form of `value`, or why it is not one.
    fn normalize(self, value: &str) -> Result<String, String> {
        match self {
            Setting::StatementTimeout => duration_ms(value).map(|ms| format!("{ms}ms")),
            Setting::WorkMem => bytes(value).map(|bytes| format!("{bytes}B")),
            Setting::DisallowFullTableScans => {
                boolean(value).map(|on| if on { "on" } else { "off" }.to_owned())
            }
            Setting::LargeFullScanRows => value
                .parse::<u64>()
                .map(|rows| rows.to_string())
                .map_err(|_| "expected a whole number of rows".to_owned()),
        }
    }
}

/// A settings statement.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Command {
    /// `SET name = value`, or `None` for `RESET name` / `SET name = DEFAULT`.
    Set(Setting, Option<String>),
    /// `RESET ALL`.
    ResetAll,
    Show(Setting),
}

/// The settings statement `sql` is, if it names one of these settings.
pub(super) fn parse(sql: &str) -> Option<Command> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let mut words = sql.splitn(2, char::is_whitespace);
    let verb = words.next()?.to_ascii_uppercase();
    let rest = words.next().unwrap_or_default().trim();
    match verb.as_str() {
        "SHOW" => Setting::of(rest).map(Command::Show),
        "RESET" if rest.eq_ignore_ascii_case("all") => Some(Command::ResetAll),
        "RESET" => Setting::of(rest).map(|setting| Command::Set(setting, None)),
        "SET" => {
            let rest = strip_word(rest, "SESSION").unwrap_or(rest);
            let (name, value) = rest

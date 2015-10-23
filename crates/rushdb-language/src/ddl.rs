//! Neo4j-style index DDL recognised before the query translator (SD-4.6): `CREATE INDEX [name] [IF NOT EXISTS] FOR (n:Label) ON (n.prop)`,
//! `CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR (n:Label) ON EACH
//! [n.prop] [OPTIONS {indexConfig: {consistency: 'immediate'}}]`,
//! `DROP INDEX name [IF EXISTS]`, and `SHOW INDEX[ES]`.

/// Kind of a declared index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlKind {
    /// Ordered (equality, ranges, prefix).
    Ordered,
    /// Full-text; `immediate` = read-your-writes, else eventual.
    Fulltext {
        /// `consistency: 'immediate'` was given.
        immediate: bool,
    },
}

/// One recognised index statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexDdl {
    /// Declare an ordered index.
    Create {
        /// Index name (`<label>_<property>` when omitted).
        name: String,
        /// Record label.
        label: String,
        /// Property name.
        property: String,
        /// Index kind.
        kind: DdlKind,
        /// `IF NOT EXISTS` was given.
        if_not_exists: bool,
    },
    /// Drop an index.
    Drop {
        /// Index name.
        name: String,
        /// `IF EXISTS` was given.
        if_exists: bool,
    },
    /// List indexes.
    Show,
}

fn words(statement: &str) -> Vec<&str> {
    statement
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect()
}

fn keyword(word: Option<&&str>, expected: &str) -> bool {
    word.is_some_and(|word| word.eq_ignore_ascii_case(expected))
}

/// Parse `statement` as index DDL: `None` when it is not one (the caller
/// runs it as a query), `Some(Err)` when it starts like one but is malformed.
#[must_use]
pub fn parse_index_ddl(statement: &str) -> Option<Result<IndexDdl, String>> {
    let w = words(statement);
    if keyword(w.first(), "SHOW") && (keyword(w.get(1), "INDEXES") || keyword(w.get(1), "INDEX")) {
        return Some(if w.len() == 2 {
            Ok(IndexDdl::Show)
        } else {
            Err("SHOW INDEXES takes no arguments".into())
        });
    }
    if keyword(w.first(), "DROP") && keyword(w.get(1), "INDEX") {
        return Some(match w.as_slice() {
            [_, _, name] => Ok(IndexDdl::Drop {
                name: (*name).to_owned(),
                if_exists: false,
            }),
            [_, _, name, if_, exists]
                if if_.eq_ignore_ascii_case("IF") && exists.eq_ignore_ascii_case("EXISTS") =>
            {
                Ok(IndexDdl::Drop {
                    name: (*name).to_owned(),
                    if_exists: true,
                })
            }
            _ => Err("expected DROP INDEX <name> [IF EXISTS]".into()),
        });
    }
    if keyword(w.first(), "CREATE") && keyword(w.get(1), "INDEX") {
        return Some(parse_create(statement, 2, DdlKind::Ordered));
    }
    if keyword(w.first(), "CREATE") && keyword(w.get(1), "FULLTEXT") && keyword(w.get(2), "INDEX") {
        let upper = statement.to_ascii_uppercase();
        let immediate = upper.contains("CONSISTENCY") && upper.contains("'IMMEDIATE'");
        return Some(parse_create(statement, 3, DdlKind::Fulltext { immediate }));
    }
    None
}

fn parse_create(statement: &str, keywords: usize, kind: DdlKind) -> Result<IndexDdl, String> {
    let malformed = || {
        "expected CREATE [FULLTEXT] INDEX [name] [IF NOT EXISTS] FOR (n:Label) ON (n.prop) | ON EACH [n.prop]"
            .to_string()
    };
    let text = statement.trim().trim_end_matches(';');
    // OPTIONS {...} only configures consistency (read by the caller).
    let text = match text.to_ascii_uppercase().find(" OPTIONS ") {
        Some(at) => &text[..at],
        None => text,
    };
    let upper = text.to_ascii_uppercase();
    let for_at = upper.find(" FOR ").ok_or_else(malformed)?;
    let head: Vec<&str> = text[..for_at].split_whitespace().skip(keywords).collect();
    let (name, if_not_exists) = match head.as_slice() {
        [] => (None, false),
        [name] => (Some((*name).to_owned()), false),
        [if_, not, exists]
            if if_.eq_ignore_ascii_case("IF")
                && not.eq_ignore_ascii_case("NOT")
                && exists.eq_ignore_ascii_case("EXISTS") =>
        {
            (None, true)

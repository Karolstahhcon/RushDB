//! Gremlin traversal bytecode (`TinkerPop` `GraphSON` 3) → Gremlin text.
//!
//! Language variants (gremlin-python, gremlin-javascript, Gremlin.Net, the
//! Java driver) send a traversal as bytecode: an ordered list of steps,
//! each a name and typed arguments. This translates it deterministically
//! into the text traversal the engine parses, so a driver's traversal runs
//! without the client writing Gremlin text.
//!
//! Accepted: `g:Bytecode` (or its untyped `{"step": [...], "source":
//! [...]}` form) with JSON scalars, `g:Int32`/`g:Int64`/`g:Float`/
//! `g:Double`, `g:List`/`g:Set`, `g:Map`, `g:P` (including `and`/`or`/
//! `not`), `g:TextP`, the `g:T`/`g:Order`/`g:Scope`/`g:Column`/
//! `g:Direction`/`g:Pop`/`g:Cardinality` enums and nested `g:Bytecode`
//! (anonymous traversals, written as bare chained steps, `where(out('x'))`:
//! valid TinkerPop grammar and what the engine's parser reads).
//! Refused: lambdas, traversal strategies and other source steps, and any
//! type not listed.

use serde_json::Value as Json;

use crate::LanguageError;

/// Nesting allowed for anonymous traversals and argument values.
const MAX_DEPTH: usize = 64;

fn unsupported(message: impl Into<String>) -> LanguageError {
    LanguageError::UnsupportedMutation(format!("gremlin bytecode: {}", message.into()))
}

/// The Gremlin text of a top-level traversal `bytecode`.
///
/// # Errors
/// Returns [`LanguageError::UnsupportedMutation`] for malformed bytecode or
/// a construct this translation does not accept.
pub fn bytecode_to_gremlin(bytecode: &Json) -> Result<String, LanguageError> {
    let (source, steps) = unwrap_bytecode(bytecode)?;
    if !source.is_empty() {
        return Err(unsupported(
            "source instructions (strategies, side effects) are not accepted",
        ));
    }
    if steps.is_empty() {
        return Err(unsupported("a traversal needs at least one step"));
    }
    let mut out = String::from("g");
    for step in steps {
        out.push('.');
        out.push_str(&instruction(step, 0)?);
    }
    Ok(out)
}

/// `(source, step)` instruction lists of a bytecode value.
fn unwrap_bytecode(value: &Json) -> Result<(&[Json], &[Json]), LanguageError> {
    let body = match typed(value) {
        Some(("g:Bytecode", inner)) => inner,
        Some((other, _)) => return Err(unsupported(format!("expected g:Bytecode, got {other}"))),
        None => value,
    };
    let list = |key: &str| -> Result<&[Json], LanguageError> {
        match body.get(key) {
            None => Ok(&[]),
            Some(Json::Array(items)) => Ok(items),
            Some(_) => Err(unsupported(format!(
                "`{key}` must be a list of instructions"
            ))),
        }
    };
    Ok((list("source")?, list("step")?))
}

/// `(@type, @value)` of a `GraphSON` typed value.
fn typed(value: &Json) -> Option<(&str, &Json)> {
    let object = value.as_object()?;
    Some((object.get("@type")?.as_str()?, object.get("@value")?))
}

fn instruction(step: &Json, depth: usize) -> Result<String, LanguageError> {
    let Some((name, args)) = step.as_array().and_then(|parts| parts.split_first()) else {
        return Err(unsupported("an instruction is a non-empty list"));
    };
    let Some(name) = name.as_str() else {
        return Err(unsupported("an instruction name is a string"));
    };
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(unsupported(format!("invalid step name `{name}`")));
    }
    let args = args
        .iter()
        .map(|arg| argument(arg, depth + 1))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("{name}({})", args.join(", ")))
}

fn argument(value: &Json, depth: usize) -> Result<String, LanguageError> {
    if depth > MAX_DEPTH {
        return Err(unsupported("nesting too deep"));
    }
    match value {
        Json::Null => Ok("null".into()),
        Json::Bool(b) => Ok(b.to_string()),
        Json::Number(n) => Ok(n.to_string()),
        Json::String(s) => Ok(quoted(s)),
        Json::Array(items) => list(items, depth),
        Json::Object(_) => match typed(value) {
            Some((kind, inner)) => typed_argument(kind, inner, depth),
            None => Err(unsupported("an untyped object argument")),
        },
    }
}

fn list(items: &[Json], depth: usize) -> Result<String, LanguageError> {
    let items = items
        .iter()
        .map(|item| argument(item, depth + 1))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("[{}]", items.join(", ")))
}

fn typed_argument(kind: &str, inner: &Json, depth: usize) -> Result<String, LanguageError> {
    match kind {
        "g:Int32" | "g:Int64" | "g:Float" | "g:Double" => match inner {
            Json::Number(n) => Ok(n.to_string()),
            _ => Err(unsupported(format!("{kind} needs a number"))),
        },
        "g:List" | "g:Set" => match inner {
            Json::Array(items) => list(items, depth),
            _ => Err(unsupported(format!("{kind} needs a list"))),
        },
        "g:Map" => {
            let Json::Array(flat) = inner else {
                return Err(unsupported("g:Map needs a flat key/value list"));
            };
            if flat.len() % 2 != 0 {
                return Err(unsupported("g:Map needs key/value pairs"));
            }
            let entries = flat
                .chunks(2)
                .map(|pair| {
                    Ok(format!(
                        "{}: {}",
                        argument(&pair[0], depth + 1)?,
                        argument(&pair[1], depth + 1)?
                    ))
                })
                .collect::<Result<Vec<_>, LanguageError>>()?;
            Ok(format!("[{}]", entries.join(", ")))
        }
        "g:P" | "g:TextP" => predicate(inner, depth),
        "g:T" => enum_value("T", inner),

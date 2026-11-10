//! Reading typed input of a command.
//!
//! Pattern: Adapter — small readers over the frontend's `Values`, so a backend names a field once.

use fs_admin_frontend::Values;
use fs_objects::ObjectId;

use crate::ids::from_iri;

/// The first value of `field`, if one was sent and is not empty.
pub fn one<'a>(input: &'a Values, field: &str) -> Option<&'a str> {
    input
        .get(field)
        .and_then(|v| v.first())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
}

/// The id in `field`.
pub fn id_of(input: &Values, field: &str) -> Option<ObjectId> {
    one(input, field).and_then(from_iri)
}

/// `on`/`yes` is `true`, `off`/`no` is `false`; anything else is none.
pub fn flag_of(text: &str) -> Option<bool> {
    match text {
        "on" | "yes" => Some(true),
        "off" | "no" => Some(false),
        _ => None,
    }
}

/// The flag in `field` of an input.
pub fn flag(input: &Values, field: &str) -> Option<bool> {
    flag_of(one(input, field)?)
}

/// The record a command works on: the id the frontend passes, else the field `id` of the input.
pub fn record_of(record: Option<&str>, input: &Values) -> Option<ObjectId> {
    record.and_then(from_iri).or_else(|| id_of(input, "id"))
}

/// Whether `record` (a name) matches the search term: empty matches all; else a case-insensitive part of it.
pub fn matches(term: &str, name: &str) -> bool {
    term.is_empty() || name.to_lowercase().contains(&term.to_lowercase())
}

/// One value, as a result entry.
pub fn single(value: impl Into<String>) -> Vec<String> {
    vec![value.into()]
}

/// A flag as a result value.
pub fn yes_no(on: bool) -> &'static str {
    if on { "yes" } else { "no" }
}

/// A flag as `on`/`off`.
pub fn on_off(on: bool) -> &'static str {
    if on { "on" } else { "off" }
}

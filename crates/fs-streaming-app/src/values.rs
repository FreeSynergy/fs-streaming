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
pub fn flag(input: &Values, field: &str) -> Option<bool> {
    match one(input, field)? {
        "on" | "yes" => Some(true),
        "off" | "no" => Some(false),
        _ => None,
    }
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

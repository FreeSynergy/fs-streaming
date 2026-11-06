//! The explicit "not possible" of a method a provider does not override.
//!
//! Pattern: Null Object — one place for the wording, so every default method answers alike (ADR 0045).

use fs_interfaces::Unsupported;

use crate::error::Result;

/// `Err(Unsupported)` for the capability id `capability`.
pub fn not_possible<T>(capability: &str) -> Result<T> {
    Err(Unsupported::new(capability, "this provider does not offer it").into())
}

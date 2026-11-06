//! A string that must never be printed: stream keys.
//!
//! Pattern: Newtype — the wrapper has no `Display` and prints `[hidden]` in `Debug`, so a key cannot
//! reach an error, a note, a log line or a feed event by accident. Reading it takes `expose`.

use std::fmt;

/// A secret text. `Debug` prints `[hidden]`; there is no `Display`.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wrap a secret text.
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    /// The text itself. Call this only where the secret must really be sent or stored.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[hidden]")
    }
}

//! Publisher keys: made once, shown once, kept only as a hash.
//!
//! Pattern: Strategy (hash and compare in one place) — 256 random bits from the system, hex text,
//! `sha256` of it kept, compared in constant time. A wrong key and an unknown stream take the same path.

use std::fmt::Write;

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::{Error, Result};
use crate::secret::Secret;

/// The hash kept of a key: `sha256:<hex>`.
pub fn hash(key: &str) -> String {
    format!("sha256:{}", hex(&Sha256::digest(key.as_bytes())))
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// A fresh publisher key: 256 random bits as 64 hex characters.
pub fn generate() -> Result<Secret> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| Error::InvalidInput("no randomness available"))?;
    Ok(Secret::new(hex(&bytes)))
}

/// Whether `key` has the kept `hash`, compared in constant time. `None` (no such stream) costs the
/// same hashing and answers `false`.
pub fn matches(kept: Option<&str>, key: &str) -> bool {
    let given = hash(key);
    let kept = kept.unwrap_or("sha256:");
    let same_len = kept.len() == given.len();
    let equal: bool = kept.as_bytes().ct_eq(given.as_bytes()).into();
    same_len && equal
}

/// Whether a key text can be sent at all: not empty, no control characters, not absurdly long.
pub fn is_sendable(key: &str) -> bool {
    !key.is_empty() && key.len() <= 1024 && !key.chars().any(|c| c.is_control() || c == '/')
}

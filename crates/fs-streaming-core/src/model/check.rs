//! `CheckResult` and `KeyCheck`: one target's answer before going live.
//!
//! Pattern: plain data — `rejected` only on an explicit no, `accepted` only on an explicit yes.

use fs_store::ObjectId;

use crate::note::Note;

/// Whether the far end took the key; only an explicit no is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyCheck {
    /// Not known.
    Unknown,
    /// The far end said yes.
    Accepted,
    /// The far end explicitly said no.
    Rejected,
}

/// The answer for one target.
#[derive(Clone, Debug, PartialEq)]
pub struct CheckResult {
    /// The target.
    pub target: ObjectId,
    /// Connection and TLS worked.
    pub reached: bool,
    /// The far end's view of the key.
    pub key: KeyCheck,
    /// In words a person understands; says so if a key travelled readable.
    pub notes: Vec<Note>,
}

impl CheckResult {
    /// The notes as one text.
    pub fn note_text(&self) -> String {
        self.notes
            .iter()
            .map(|n| n.text())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

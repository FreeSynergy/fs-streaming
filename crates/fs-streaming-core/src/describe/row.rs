//! One row of the command table.
//!
//! Pattern: Builder — `Row::new(..).input(..).planned("why")`; a row is a value (name, kind, input, result,
//! `supported`, reason) that the table and its test read.

use fs_types::{CommandDescription, CommandKind, FieldDescription};

/// The data type a command belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owner {
    /// `Stream`.
    Stream,
    /// `Target`.
    Target,
}

impl Owner {
    /// The name of the data type in the catalog.
    pub const fn type_name(self) -> &'static str {
        match self {
            Self::Stream => "Stream",
            Self::Target => "Target",
        }
    }

    /// The first word of every translation key of the type.
    pub const fn key(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Target => "target",
        }
    }
}

/// `setRecording` -> `set-recording`: the translation key part of a camel-case name.
pub(super) fn kebab(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_uppercase() {
            out.push('-');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// A command with the answer to "can this program do it now".
#[derive(Clone, Debug)]
pub struct Row {
    /// The data type it is on.
    pub owner: Owner,
    /// The capability ids of `requirements.md` it carries out (may be none for a command of the contract only).
    pub capabilities: &'static [&'static str],
    /// Name, kind, label, input and result.
    pub command: CommandDescription,
    /// The core does it and the app offers it.
    pub supported: bool,
    /// Why not, in English for the developer; empty when supported.
    pub reason: &'static str,
}

impl Row {
    /// A supported command with no input yet.
    pub fn new(
        owner: Owner,
        name: &str,
        kind: CommandKind,
        capabilities: &'static [&'static str],
    ) -> Self {
        let label = format!("{}.{}", owner.key(), kebab(name));
        Self {
            owner,
            capabilities,
            command: CommandDescription::new(name, kind, &label),
            supported: true,
            reason: "",
        }
    }

    /// Add an input field.
    #[must_use]
    pub fn input(mut self, field: FieldDescription) -> Self {
        self.command = self.command.input(field);
        self
    }

    /// Add a result field.
    #[must_use]
    pub fn result(mut self, field: FieldDescription) -> Self {
        self.command = self.command.result(field);
        self
    }

    /// Mark the row as not possible, with the reason.
    #[must_use]
    pub fn planned(mut self, reason: &'static str) -> Self {
        self.supported = false;
        self.reason = reason;
        self
    }
}

//! Short constructors for the fields of the two types and of their commands.
//!
//! Pattern: Builder helpers — one place for the translation-key scheme `<type>.<name>` (fields) and
//! `<type>.<command>.<name>` (inputs and results); the publisher key and a target key have no field anywhere.

use fs_types::{BasicType, FieldDescription};

use super::row::{Owner, kebab};

/// A field of the type: `stream.name`.
pub(super) fn field(owner: Owner, name: &str, basic: BasicType) -> FieldDescription {
    FieldDescription::new(name, basic).label(&format!("{}.{}", owner.key(), kebab(name)))
}

/// An input or result of a command: `stream.rename.name`.
pub(super) fn arg(owner: Owner, command: &str, name: &str, basic: BasicType) -> FieldDescription {
    FieldDescription::new(name, basic).label(&format!(
        "{}.{}.{}",
        owner.key(),
        kebab(command),
        kebab(name)
    ))
}

/// The record a command works on: a reference to the own type, required (the rule for commands on one record).
pub(super) fn id(owner: Owner, command: &str) -> FieldDescription {
    arg(owner, command, "id", BasicType::Reference)
        .target(owner.type_name())
        .required()
}

/// A reference to a stream, as input of a command of `owner`.
pub(super) fn stream_ref(owner: Owner, command: &str, name: &str) -> FieldDescription {
    arg(owner, command, name, BasicType::Reference).target("Stream")
}

/// A yes/no flag. There is no boolean basic type yet, so it is a choice of two keys.
pub(super) fn flag(f: FieldDescription, on: &str, off: &str) -> FieldDescription {
    f.choices(&[on, off])
}

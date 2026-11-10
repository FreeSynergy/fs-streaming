//! The commands of `Target` that the contract has, and `target.connect`.
//!
//! Pattern: Registry entry — rows of the table. A target key is a secret: `add` takes none, and `setKey` is held
//! back, because a form would show the typed key and keep it in the form model; fs-types has no masked kind.

use fs_types::{BasicType, CommandDescription, CommandKind};

use super::fields::{arg, flag, id, stream_ref};
use super::row::{Owner, Row};

const T: Owner = Owner::Target;

const MASKED: &str = "the core does it, but a target key is a secret and the admin frontend has no masked input that keeps it out of forms and records, so the app does not run it; the key is left out of its input on purpose";

/// Rows for targets.
pub(super) fn rows() -> Vec<Row> {
    vec![
        // The standard `find`; the filter by stream is a further input field (rule in `types.capnp`).
        Row::from_command(
            T,
            &[],
            CommandDescription::standard_find("target.find", "Target")
                .input(stream_ref(T, "find", "stream")),
        ),
        // `restream.add` is: add, edit, enable or disable, remove. The key is set apart (`setKey`).
        Row::new(T, "add", CommandKind::Create, &["restream.add"])
            .input(stream_ref(T, "add", "stream").required())
            .input(arg(T, "add", "name", BasicType::Text).required())
            .input(arg(T, "add", "url", BasicType::Link).required())
            .result(id(T, "add")),
        // `change` is an `Edit`: name, address, and the flag `enabled`, field by field.
        Row::new(T, "change", CommandKind::Change, &[]),
        Row::new(T, "setEnabled", CommandKind::Other, &[])
            .input(id(T, "setEnabled"))
            .input(flag(
                arg(T, "setEnabled", "enabled", BasicType::Choice).required(),
                "on",
                "off",
            )),
        Row::new(T, "remove", CommandKind::Delete, &[]).input(id(T, "remove")),
        Row::new(T, "setKey", CommandKind::Other, &[]).input(id(T, "setKey")).planned(MASKED),
        Row::new(T, "connect", CommandKind::Create, &["target.connect"])
            .input(stream_ref(T, "connect", "stream").required())
            .input(arg(T, "connect", "name", BasicType::Text).required())
            .planned("publishing through a platform account (OAuth) is not built; a target is URL and key"),
    ]
}

//! The commands of `Stream` that the contract has (`Streams` in `streaming.capnp`).
//!
//! Pattern: Registry entry — rows of the table, one per method of the contract; the publisher key is never a
//! field: `create` and `rotateKey` are held back (see their reason) because a key shown once needs a display that
//! keeps it out of records, and the frontend has none.

use fs_types::{BasicType, CommandKind};

use super::fields::{arg, flag, id};
use super::row::{Owner, Row};

const S: Owner = Owner::Stream;

const KEY_ONCE: &str = "the core does it, but the publisher key is shown once and never in a field value, record or About; the admin frontend has no one-time secret display, so the app does not run it";

/// Rows for the methods of `Streams` that concern a stream.
pub(super) fn rows() -> Vec<Row> {
    vec![
        // The list is `find` with an empty query (decision of the Strategy); the core pages by cursor.
        Row::new(S, "find", CommandKind::Find, &["stream.list"]).result(
            arg(S, "find", "streams", BasicType::Reference)
                .target("Stream")
                .multiple(),
        ),
        Row::new(S, "create", CommandKind::Create, &["stream.create"])
            .input(arg(S, "create", "name", BasicType::Text).required())
            .result(
                arg(S, "create", "id", BasicType::Reference)
                    .target("Stream")
                    .required(),
            )
            .result(arg(S, "create", "ingestUrl", BasicType::Link))
            .planned(KEY_ONCE),
        Row::new(S, "rename", CommandKind::Change, &[])
            .input(id(S, "rename"))
            .input(arg(S, "rename", "name", BasicType::Text).required())
            .result(id(S, "rename")),
        Row::new(S, "delete", CommandKind::Delete, &["stream.delete"]).input(id(S, "delete")),
        Row::new(S, "rotateKey", CommandKind::Other, &["publisher.authorize"])
            .input(id(S, "rotateKey"))
            .planned(KEY_ONCE),
        Row::new(S, "stop", CommandKind::Other, &[]).input(id(S, "stop")),
        Row::new(S, "setRecording", CommandKind::Other, &["recording.start"])
            .input(id(S, "setRecording"))
            .input(flag(
                arg(S, "setRecording", "recording", BasicType::Choice).required(),
                "on",
                "off",
            )),
        Row::new(S, "check", CommandKind::Check, &["target.check"])
            .input(id(S, "check"))
            .result(
                arg(S, "check", "target", BasicType::Reference)
                    .target("Target")
                    .multiple(),
            )
            .result(flag(
                arg(S, "check", "reached", BasicType::Choice).multiple(),
                "yes",
                "no",
            ))
            .result(
                arg(S, "check", "keyCheck", BasicType::Choice)
                    .multiple()
                    .choices(&["unknown", "accepted", "rejected"]),
            )
            .result(arg(S, "check", "note", BasicType::Text).multiple()),
        Row::new(S, "status", CommandKind::Other, &["stats.read"])
            .input(id(S, "status"))
            .result(arg(S, "status", "state", BasicType::Choice).choices(&super::stream::STATES))
            .result(arg(S, "status", "since", BasicType::DateTime))
            .result(arg(S, "status", "bytesIn", BasicType::Text))
            .result(flag(
                arg(S, "status", "hasVideo", BasicType::Choice),
                "yes",
                "no",
            ))
            .result(flag(
                arg(S, "status", "hasAudio", BasicType::Choice),
                "yes",
                "no",
            ))
            .result(arg(S, "status", "recordingProblem", BasicType::Text)),
    ]
}

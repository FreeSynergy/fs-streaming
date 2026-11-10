//! The description of `Stream`.
//!
//! Pattern: Registry entry (trait first) — [`Stream`] implements `DescribesType` of `fs-types`, so
//! `TypeRegistry::register::<Stream>()` puts it in the catalog. The standard: no data standard covers a live
//! stream (`docs/en/data/` has no streaming domain); the contract is `streaming.capnp`, ingest is RTMP.
//! No field holds a key: the publisher key is a secret shown once and has no field anywhere.

use fs_types::{BasicType, DescribesType, TypeDescription};

use super::fields::{field, flag};
use super::row::Owner;
use super::table::commands_of;
use crate::model::Stream;

/// The keys of `StreamState`, in the order of the contract.
pub(super) const STATES: [&str; 6] = ["idle", "checking", "live", "ended", "error", "interrupted"];

const S: Owner = Owner::Stream;

impl DescribesType for Stream {
    fn describe() -> TypeDescription {
        let mut t = TypeDescription::new(
            "Stream",
            "fs-streaming",
            "none (own type, streaming.capnp; RTMP ingest)",
            "stream",
        )
        .field(field(S, "name", BasicType::Text).required())
        .field(field(S, "state", BasicType::Choice).choices(&STATES))
        .field(flag(field(S, "recording", BasicType::Choice), "on", "off"))
        .field(field(S, "note", BasicType::Text))
        .field(field(S, "ingestUrl", BasicType::Link));
        for c in commands_of(S) {
            t = t.command(c);
        }
        t
    }
}

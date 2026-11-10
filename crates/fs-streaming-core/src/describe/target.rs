//! The description of `Target`.
//!
//! Pattern: Registry entry (trait first) — [`Target`] implements `DescribesType` of `fs-types`. The standard: a
//! target is an RTMP or RTMPS address (the open RTMP specification); no data standard covers it. `hasKey` says
//! whether a key is set; the key itself has no field.

use fs_types::{BasicType, DescribesType, TypeDescription};

use super::fields::field;
use super::row::Owner;
use super::table::commands_of;
use crate::model::Target;

const T: Owner = Owner::Target;

impl DescribesType for Target {
    fn describe() -> TypeDescription {
        let yes_no = ["yes", "no"];
        let mut t = TypeDescription::new(
            "Target",
            "fs-streaming",
            "none (own type, streaming.capnp; RTMP and RTMPS addresses)",
            "target",
        )
        .field(
            field(T, "stream", BasicType::Reference)
                .target("Stream")
                .required(),
        )
        .field(field(T, "name", BasicType::Text).required())
        .field(field(T, "url", BasicType::Link).required())
        .field(field(T, "hasKey", BasicType::Choice).choices(&yes_no))
        .field(field(T, "enabled", BasicType::Choice).choices(&["on", "off"]))
        .field(field(T, "secure", BasicType::Choice).choices(&yes_no))
        .field(field(T, "state", BasicType::Choice).choices(&[
            "off",
            "waiting",
            "connecting",
            "sending",
            "retrying",
            "refused",
        ]))
        .field(field(T, "note", BasicType::Text));
        for c in commands_of(T) {
            t = t.command(c);
        }
        t
    }
}

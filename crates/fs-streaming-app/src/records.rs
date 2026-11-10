//! The core's `Stream` and `Target` as records of the frontend.
//!
//! Pattern: Adapter — typed values by field name. A key is not in a record: a stream shows its address without it,
//! a target shows only whether a key is set.

use fs_admin_frontend::{Record, Values};
use fs_streaming_core::{Stream, StreamState, Target, TargetState};

use crate::ids::to_iri;
use crate::values::{on_off, single, yes_no};

const fn stream_state(s: StreamState) -> &'static str {
    match s {
        StreamState::Idle => "idle",
        StreamState::Checking => "checking",
        StreamState::Live => "live",
        StreamState::Ended => "ended",
        StreamState::Error => "error",
        StreamState::Interrupted => "interrupted",
    }
}

const fn target_state(s: TargetState) -> &'static str {
    match s {
        TargetState::Off => "off",
        TargetState::Waiting => "waiting",
        TargetState::Connecting => "connecting",
        TargetState::Sending => "sending",
        TargetState::Retrying => "retrying",
        TargetState::Refused => "refused",
    }
}

/// The state key of a stream, for a result.
pub fn stream_state_key(s: StreamState) -> &'static str {
    stream_state(s)
}

/// A stream as a record. The note is the core's fixed sentence.
pub fn stream(s: &Stream) -> Record {
    let mut v = Values::new();
    v.insert("name".into(), single(&s.name));
    v.insert("state".into(), single(stream_state(s.state)));
    v.insert("recording".into(), single(on_off(s.recording)));
    if let Some(n) = s.note {
        v.insert("note".into(), single(n.text()));
    }
    if !s.ingest_url.is_empty() && !s.ingest_url.starts_with('/') {
        v.insert("ingestUrl".into(), single(&s.ingest_url));
    }
    Record {
        id: to_iri(s.id),
        values: v,
    }
}

/// A target as a record. `hasKey` is a flag; the key is not here.
pub fn target(t: &Target) -> Record {
    let mut v = Values::new();
    v.insert("stream".into(), single(to_iri(t.stream)));
    v.insert("name".into(), single(&t.name));
    v.insert("url".into(), single(&t.url));
    v.insert("hasKey".into(), single(yes_no(t.has_key)));
    v.insert("enabled".into(), single(on_off(t.enabled)));
    v.insert("secure".into(), single(yes_no(t.secure)));
    v.insert("state".into(), single(target_state(t.state)));
    if let Some(n) = t.note {
        v.insert("note".into(), single(n.text()));
    }
    Record {
        id: to_iri(t.id),
        values: v,
    }
}

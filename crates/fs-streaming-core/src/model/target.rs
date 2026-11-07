//! `Target` and `TargetState` of the contract.
//!
//! Pattern: State (the enum; transitions live in `state::target`) — plain data otherwise.

use fs_objects::ObjectId;

use crate::note::Note;

/// What a target is doing. The Rust form of `TargetState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TargetState {
    /// Switched off.
    Off,
    /// The stream is not live.
    Waiting,
    /// Connecting now.
    Connecting,
    /// Sending media.
    Sending,
    /// Lost; tries again with growing pauses.
    Retrying,
    /// The far end explicitly said no; no more tries until edited, switched on again, or the next live.
    Refused,
}

/// A target as shown to callers. The key is not here: `has_key` says whether one is set.
#[derive(Clone, Debug, PartialEq)]
pub struct Target {
    /// Id.
    pub id: ObjectId,
    /// The stream it belongs to.
    pub stream: ObjectId,
    /// Name.
    pub name: String,
    /// `rtmp://` or `rtmps://`, host and application, never the key.
    pub url: String,
    /// A key is set.
    pub has_key: bool,
    /// Switched on.
    pub enabled: bool,
    /// rtmps; a plain rtmp target travels readable, the key included.
    pub secure: bool,
    /// State now.
    pub state: TargetState,
    /// The last problem, with no key in it.
    pub note: Option<Note>,
}

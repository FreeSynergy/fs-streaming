//! `Status`: what a running stream is doing now; nothing of it is stored.
//!
//! Pattern: plain data — filled from the core's runtime state and the relay's counters.

use fs_store::{ObjectId, Timestamp};

use super::{StreamState, TargetState};
use crate::note::Note;

/// Counters and state of one stream.
#[derive(Clone, Debug, PartialEq)]
pub struct Status {
    /// State now.
    pub state: StreamState,
    /// Since when it is in this state.
    pub since: Timestamp,
    /// Bytes received from the sender.
    pub bytes_in: u64,
    /// One entry per target.
    pub targets: Vec<TargetStatus>,
    /// A video message was seen.
    pub has_video: bool,
    /// An audio message was seen.
    pub has_audio: bool,
    /// Why the recording is not working; none if it is fine. A failing recording does not stop the stream.
    pub recording_problem: Option<Note>,
}

/// Counters and state of one target.
#[derive(Clone, Debug, PartialEq)]
pub struct TargetStatus {
    /// The target.
    pub target: ObjectId,
    /// State now.
    pub state: TargetState,
    /// Bytes sent to it.
    pub bytes_out: u64,
    /// Frames left out for this target because it could not keep up.
    pub dropped: u64,
    /// Connections made after the first.
    pub reconnects: u32,
    /// The last problem.
    pub note: Option<Note>,
}

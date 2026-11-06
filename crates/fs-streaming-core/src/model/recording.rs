//! `RecordingInfo`: a finished recording, as the sink reports it.
//!
//! Pattern: plain data — `file_id` stays unset until a sink hands over to fs-files.

use fs_store::{ObjectId, Timestamp};

/// What is known when a recording starts.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordingStart {
    /// The stream.
    pub stream: ObjectId,
    /// The stream's name.
    pub name: String,
    /// When the recording started.
    pub from: Timestamp,
}

/// A finished recording.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordingInfo {
    /// The stream.
    pub stream: ObjectId,
    /// Name.
    pub name: String,
    /// Where the sink put it (a path, or an IRI at fs-files).
    pub file: String,
    /// At fs-files; unset if the sink is not fs-files.
    pub file_id: Option<ObjectId>,
    /// Size in bytes.
    pub bytes: u64,
    /// Start.
    pub from: Timestamp,
    /// End.
    pub to: Timestamp,
}

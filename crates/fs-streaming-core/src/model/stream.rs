//! `Stream` and `StreamState` of the contract.
//!
//! Pattern: State (the enum; transitions live in `state::stream`) — plain data otherwise.

use fs_store::ObjectId;

use crate::note::Note;
use crate::secret::Secret;

/// Where a stream is in its life. The Rust form of `StreamState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamState {
    /// Nobody is sending.
    Idle,
    /// A check of the targets is running.
    Checking,
    /// A sender is connected.
    Live,
    /// A sender was live before and is gone.
    Ended,
    /// Something went wrong; see the note.
    Error,
    /// The sender dropped a moment ago; targets and recording stay open for a while.
    Interrupted,
}

/// A stream as shown to callers.
#[derive(Clone, Debug, PartialEq)]
pub struct Stream {
    /// Id.
    pub id: ObjectId,
    /// Name.
    pub name: String,
    /// State now (live only; not stored).
    pub state: StreamState,
    /// Records when it goes live, and while it is.
    pub recording: bool,
    /// Why, for `Error`.
    pub note: Option<Note>,
    /// Where a sender connects; the key is not part of it.
    pub ingest_url: String,
}

/// The answer of `create`: the key is in here once and never again.
#[derive(Clone, Debug)]
pub struct Created {
    /// The new stream.
    pub stream: Stream,
    /// Where a sender connects.
    pub ingest_url: String,
    /// The publisher key, shown this once.
    pub publisher_key: Secret,
}

//! `fs-streaming-core`: program core of FreeSynergy streaming (no media, no sockets, no capnp server).
//!
//! A stream has a publisher key (shown once, kept as a hash) and targets (with keys kept only in a
//! private store). The state machines, the refusals of the contract and the storage are here; the
//! media side is behind the ports [`Relay`] and [`Gate`], recordings behind [`RecordingSink`].
//! Start with [`StreamService`]. See `DESIGN.md`.
//!
//! Pattern: Facade — this file only declares modules and re-exports the public API.

#![deny(missing_docs)]
#![warn(clippy::pedantic)]
// Pedantic lints that would only add noise to this crate's small, documented API.
#![allow(
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::module_name_repetitions
)]

mod cancel;
pub mod describe;
mod error;
pub mod keys;
pub mod model;
mod note;
mod repo;
mod secret;
mod service;
pub mod state;
pub mod traits;
mod unsupported;
mod url;

pub use cancel::CancelToken;
pub use error::{Error, Result};
pub use fs_objects::{ObjectId, Timestamp};
pub use model::{
    CheckResult, Created, Cursor, KeyCheck, Page, RecordingInfo, RecordingStart, Status, Stream,
    StreamState, Target, TargetState, TargetStatus,
};
pub use note::Note;
pub use repo::{Repo, StreamRow, TargetRow};
pub use secret::Secret;
pub use service::StreamService;
pub use state::Refusal;
pub use traits::{
    Event, Gate, IngestGrant, RecordingEvent, RecordingSink, RecordingWriter, Relay, RelayStatus,
    RelayTargetEvent, RelayTargetStatus, SinkError, StatusListener, Streaming, Streams,
    Subscription, TargetSpec,
};
pub use unsupported::not_possible;
pub use url::{TargetUrl, UrlProblem};

/// How long targets stay connected and the recording stays open after the sender dropped.
pub const INTERRUPT_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

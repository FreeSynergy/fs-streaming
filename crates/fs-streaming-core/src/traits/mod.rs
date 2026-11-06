//! The traits of the core: what callers use, and the ports the core uses.
//!
//! Pattern: Facade (declarations) — `Streams` and `Streaming` face callers; `Relay`, `Gate` and
//! `RecordingSink` are the ports to the media side.

mod recording;
mod relay;
mod streaming;
mod streams;

pub use recording::{RecordingSink, RecordingWriter, SinkError};
pub use relay::{
    Gate, IngestGrant, RecordingEvent, Relay, RelayStatus, RelayTargetEvent, RelayTargetStatus,
    TargetSpec,
};
pub use streaming::{Event, StatusListener, Streaming, Subscription};
pub use streams::Streams;

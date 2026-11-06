//! The ports between the core and the media side.
//!
//! Pattern: Strategy + Observer. `Relay` is what the core asks of the media side (the Strategy the
//! relay crate implements); `Gate` is what the media side asks and reports back to the core. No
//! media and no sockets are named here.

use async_trait::async_trait;
use fs_store::ObjectId;

use crate::model::{CheckResult, RecordingInfo};
use crate::note::Note;
use crate::secret::Secret;
use crate::url::TargetUrl;

/// What the relay needs to send to one target. `Debug` hides the key.
#[derive(Clone, Debug)]
pub struct TargetSpec {
    /// Target id.
    pub id: ObjectId,
    /// Where to.
    pub url: TargetUrl,
    /// The key, if any.
    pub key: Option<Secret>,
}

/// The answer to a sender that gave the right key.
#[derive(Clone, Debug)]
pub struct IngestGrant {
    /// The stream.
    pub stream: ObjectId,
    /// Its name, for recordings.
    pub name: String,
    /// Whether to record.
    pub recording: bool,
    /// The enabled targets.
    pub targets: Vec<TargetSpec>,
}

/// A report about one target's connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayTargetEvent {
    /// Connecting.
    Connecting,
    /// Sending.
    Sending,
    /// Lost; will try again.
    Lost(Note),
    /// Explicit no from the far end.
    Refused(Note),
}

/// A report about a recording.
#[derive(Clone, Debug)]
pub enum RecordingEvent {
    /// The recording works, or stopped working.
    Problem(Option<Note>),
    /// A recording was closed.
    Finished(RecordingInfo),
}

/// Counters of one target, from the relay.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RelayTargetStatus {
    /// Bytes sent.
    pub bytes_out: u64,
    /// Frames left out.
    pub dropped: u64,
    /// Connections after the first.
    pub reconnects: u32,
}

/// Counters of one running stream, from the relay.
#[derive(Clone, Debug, Default)]
pub struct RelayStatus {
    /// Bytes received.
    pub bytes_in: u64,
    /// A video message was seen.
    pub has_video: bool,
    /// An audio message was seen.
    pub has_audio: bool,
    /// Per target.
    pub targets: Vec<(ObjectId, RelayTargetStatus)>,
}

/// What the core asks of the media side.
#[async_trait]
pub trait Relay: Send + Sync {
    /// The address senders connect to, without the stream id and the key.
    fn ingest_base(&self) -> String;
    /// End the sender of a stream and everything that follows from it; cancel a check.
    async fn stop_sender(&self, stream: ObjectId);
    /// Add or change a target of a live stream. A new key is used from the next connection on.
    async fn apply_target(&self, stream: ObjectId, target: TargetSpec, enabled: bool);
    /// Forget a target.
    async fn drop_target(&self, target: ObjectId);
    /// Start or stop recording of a live stream.
    async fn set_recording(&self, stream: ObjectId, on: bool);
    /// Counters of a running stream.
    async fn status(&self, stream: ObjectId) -> Option<RelayStatus>;
    /// Try the targets without sending media.
    async fn check(&self, stream: ObjectId, targets: Vec<TargetSpec>) -> Vec<CheckResult>;
    /// A stream was deleted.
    async fn forget(&self, stream: ObjectId);
}

/// What the media side asks and reports to the core.
#[async_trait]
pub trait Gate: Send + Sync {
    /// Check a sender's key. A wrong key and an unknown stream both give `None`.
    async fn authorize(&self, stream: ObjectId, key: &str) -> Option<IngestGrant>;
    /// A sender was accepted.
    async fn sender_connected(&self, stream: ObjectId);
    /// The sender dropped; the stream waits.
    async fn sender_lost(&self, stream: ObjectId);
    /// The stream is over: stop, or the time after a drop ran out.
    async fn sender_ended(&self, stream: ObjectId);
    /// A target's connection changed.
    async fn target_event(&self, target: ObjectId, event: RelayTargetEvent);
    /// Something about the recording.
    async fn recording_event(&self, stream: ObjectId, event: RecordingEvent);
}

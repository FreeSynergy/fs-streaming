//! The root of the contract: capabilities and the feed.
//!
//! Pattern: Observer + Null Object — `subscribe` hands events to a listener; a provider without a
//! feed answers `Unsupported`. The Rust form of `interface Streaming` and `StatusListener`.

use std::sync::Arc;

use async_trait::async_trait;
use fs_interfaces::Capabilities;
use fs_store::ObjectId;
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::model::{RecordingInfo, Stream, Target};
use crate::unsupported::not_possible;

/// What the feed carries.
#[derive(Clone, Debug)]
pub enum Event {
    /// A stream changed.
    Stream(Stream),
    /// A target changed.
    Target(Target),
    /// A recording was finished.
    Recording(RecordingInfo),
    /// A stream or a target is gone.
    Removed(ObjectId),
}

/// Someone who wants to hear of changes. The Rust form of `StatusListener`.
#[async_trait]
pub trait StatusListener: Send + Sync {
    /// A stream changed.
    async fn stream(&self, stream: Stream);
    /// A target changed.
    async fn target(&self, target: Target);
    /// A recording was finished.
    async fn recording(&self, recording: RecordingInfo);
    /// A stream or a target is gone.
    async fn removed(&self, id: ObjectId);
    /// The subscription is over.
    async fn done(&self);
}

/// A running subscription; dropping it ends it.
pub struct Subscription(pub(crate) JoinHandle<()>);

impl Drop for Subscription {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The root of the streaming contract.
#[async_trait]
pub trait Streaming: Send + Sync {
    /// What this provider can do, per capability id.
    fn capabilities(&self) -> Capabilities;
    /// `stream.subscribe`: hear of changes.
    async fn subscribe(&self, _listener: Arc<dyn StatusListener>) -> Result<Subscription> {
        not_possible("stream.subscribe")
    }
}

//! The `Streams` contract: streams, targets, recording, check.
//!
//! Pattern: Repository (interface) + Null Object — callers use `dyn Streams`. Every provider has every
//! method; one it does not override answers an explicit `Unsupported` (ADR 0045). Reads are required.

use async_trait::async_trait;
use fs_store::ObjectId;

use crate::error::Result;
use crate::model::{CheckResult, Created, Cursor, Page, Status, Stream, Target};
use crate::secret::Secret;
use crate::unsupported::not_possible;

/// Streams and their targets. Interface status: draft (ADR 0045).
#[async_trait]
pub trait Streams: Send + Sync {
    /// `stream.read`: one stream, if it exists.
    async fn get(&self, id: ObjectId) -> Result<Option<Stream>>;
    /// `stream.read`: streams in creation order, one page.
    async fn list(&self, cursor: Option<&Cursor>) -> Result<Page<Stream>>;
    /// `stream.read`: what a stream is doing now.
    async fn status(&self, id: ObjectId) -> Result<Status>;
    /// `stream.read`: the targets of a stream.
    async fn targets(&self, stream: ObjectId) -> Result<Vec<Target>>;

    /// `stream.create`: a new stream; the publisher key is in the answer once.
    async fn create(&self, _name: &str) -> Result<Created> {
        not_possible("stream.create")
    }
    /// `publisher.authorize`: a new key; the old one stops at once and a running sender is ended.
    async fn rotate_key(&self, _id: ObjectId) -> Result<Secret> {
        not_possible("publisher.authorize")
    }
    /// `stream.create`: change the name.
    async fn rename(&self, _id: ObjectId, _name: &str) -> Result<Stream> {
        not_possible("stream.create")
    }
    /// `stream.create`: delete with its targets; refused while live, interrupted or checking.
    async fn delete(&self, _id: ObjectId) -> Result<()> {
        not_possible("stream.create")
    }
    /// `stream.stop`: end a running sender or cancel a check; the sender can connect again.
    async fn stop(&self, _id: ObjectId) -> Result<Stream> {
        not_possible("stream.stop")
    }
    /// `recording.start`: record or not; takes effect at once on a live stream.
    async fn set_recording(&self, _id: ObjectId, _on: bool) -> Result<Stream> {
        not_possible("recording.start")
    }
    /// `restream.add`: a new target. An empty key means none.
    async fn add_target(
        &self,
        _stream: ObjectId,
        _name: &str,
        _url: &str,
        _key: &str,
    ) -> Result<Target> {
        not_possible("restream.add")
    }
    /// `restream.add`: change name and address; refused while it is sending.
    async fn edit_target(&self, _id: ObjectId, _name: &str, _url: &str) -> Result<Target> {
        not_possible("restream.add")
    }
    /// `restream.add`: replace the key, used from the next connection on; empty removes it.
    async fn set_target_key(&self, _id: ObjectId, _key: &str) -> Result<Target> {
        not_possible("restream.add")
    }
    /// `restream.add`: switch a target on or off.
    async fn set_target_enabled(&self, _id: ObjectId, _enabled: bool) -> Result<Target> {
        not_possible("restream.add")
    }
    /// `restream.add`: remove a target.
    async fn remove_target(&self, _id: ObjectId) -> Result<()> {
        not_possible("restream.add")
    }
    /// `stream.check`: try each enabled target without sending media; refused while live.
    async fn check(&self, _stream: ObjectId) -> Result<Vec<CheckResult>> {
        not_possible("stream.check")
    }
}

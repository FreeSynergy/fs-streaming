//! What this provider reports it can do, and the feed.
//!
//! Pattern: Null Object (report) — one place lists every capability id of the contract; what the
//! contract leaves out says so plainly with a reason (ADR 0045). Also `subscribe` (Observer).

use std::sync::Arc;

use async_trait::async_trait;
use fs_interfaces::Capabilities;

use super::handle::StreamService;
use crate::error::Result;
use crate::traits::{Event, StatusListener, Streaming, Subscription};

const SUPPORTED: [&str; 8] = [
    "stream.create",
    "stream.read",
    "stream.stop",
    "stream.check",
    "stream.subscribe",
    "publisher.authorize",
    "recording.start",
    "restream.add",
];

const LEFT_OUT: [(&str, &str); 14] = [
    (
        "source.pull",
        "not in stage 1: nothing is pulled from outside",
    ),
    (
        "viewer.authorize",
        "not in stage 1: nobody watches through this program",
    ),
    ("hook.send", "not in stage 1: no hooks on stream events"),
    (
        "ingest.rtmps",
        "ingest is plain rtmp; put your own TLS in front",
    ),
    ("ingest.srt", "not built: other ways in are left out"),
    ("ingest.rtsp", "not built: other ways in are left out"),
    ("ingest.whip", "not built: other ways in are left out"),
    ("egress.hls", "not built: other ways out are left out"),
    ("egress.dash", "not built: other ways out are left out"),
    ("egress.whep", "not built: other ways out are left out"),
    (
        "recording.segments",
        "not built: a recording is one FLV file",
    ),
    (
        "publisher.authorize.other",
        "senders are authorised by key only",
    ),
    (
        "check.format",
        "not built: no format check against platforms",
    ),
    (
        "publish.offline",
        "not built: publishing a stream that is not live",
    ),
];

/// Capability ids that are not offered because the program does not decode or encode.
const NO_MEDIA: [&str; 3] = ["capture", "codec.decode", "codec.encode"];

impl StreamService {
    pub(super) fn support() -> Capabilities {
        let mut c = Capabilities::new("fs-streaming-core");
        for id in SUPPORTED {
            c.supported(id);
        }
        for (id, why) in LEFT_OUT {
            c.unsupported(id, why);
        }
        for id in NO_MEDIA {
            c.unsupported(
                id,
                "stage 1 forwards and records without decoding or encoding",
            );
        }
        c
    }
}

#[async_trait]
impl Streaming for StreamService {
    fn capabilities(&self) -> Capabilities {
        Self::support()
    }

    async fn subscribe(&self, listener: Arc<dyn StatusListener>) -> Result<Subscription> {
        let mut rx = self.feed.subscribe();
        let task = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(Event::Stream(s)) => listener.stream(s).await,
                    Ok(Event::Target(t)) => listener.target(t).await,
                    Ok(Event::Recording(r)) => listener.recording(r).await,
                    Ok(Event::Removed(id)) => listener.removed(id).await,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(_) => break,
                }
            }
            listener.done().await;
        });
        Ok(Subscription(task))
    }
}

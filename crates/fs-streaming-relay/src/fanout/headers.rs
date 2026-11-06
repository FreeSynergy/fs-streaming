//! The codec headers kept in memory.
//!
//! Pattern: Memento-like cache — the latest metadata message and the latest video and audio sequence
//! headers, sent first to anyone who joins, reconnects or starts recording.

use std::sync::Arc;

use crate::frame::{Frame, Kind};

/// Metadata and sequence headers of the running stream.
#[derive(Clone, Debug, Default)]
pub struct Headers {
    meta: Option<Arc<Frame>>,
    video: Option<Arc<Frame>>,
    audio: Option<Arc<Frame>>,
}

impl Headers {
    /// Remember `frame` if it is a header.
    pub fn update(&mut self, frame: &Arc<Frame>) {
        if !frame.header {
            return;
        }
        match frame.kind {
            Kind::Data => self.meta = Some(frame.clone()),
            Kind::Video => self.video = Some(frame.clone()),
            Kind::Audio => self.audio = Some(frame.clone()),
        }
    }

    /// In the order they go out: metadata, video, audio.
    pub fn in_order(&self) -> impl Iterator<Item = &Arc<Frame>> {
        [&self.meta, &self.video, &self.audio].into_iter().flatten()
    }
}

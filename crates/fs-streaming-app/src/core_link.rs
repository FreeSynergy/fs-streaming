//! The link from the synchronous frontend to the asynchronous core.
//!
//! Pattern: Adapter — the frontend calls `Backend` from its own thread; the core is async. The link holds the
//! core's `Streams` and a runtime handle and blocks the calling thread for one call. It must not be called from
//! inside the runtime (a window thread never is).

use std::future::Future;
use std::sync::Arc;

use fs_streaming_core::{Error, Stream, Streams};
use tokio::runtime::Handle;

/// The core and the runtime that drives it.
#[derive(Clone)]
pub struct CoreLink {
    streams: Arc<dyn Streams>,
    handle: Handle,
}

impl CoreLink {
    /// A link over `streams`, driven by `handle`.
    pub fn new(streams: Arc<dyn Streams>, handle: Handle) -> Self {
        Self { streams, handle }
    }

    /// The core.
    pub fn streams(&self) -> &dyn Streams {
        &*self.streams
    }

    /// Run one call to the end.
    pub fn wait<T>(&self, call: impl Future<Output = T>) -> T {
        self.handle.block_on(call)
    }
}

impl CoreLink {
    /// Every stream, page after page.
    pub fn all_streams(&self) -> Result<Vec<Stream>, Error> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.wait(self.streams().list(cursor.as_ref()))?;
            out.extend(page.items);
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(out),
            }
        }
    }
}

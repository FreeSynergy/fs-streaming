//! The stream a connection feeds.
//!
//! Pattern: RAII / Guard — dropping it without `release` still tells the live stream the sender is
//! gone, so a stream is never left `live` by a connection that ended in any way.

use std::sync::Arc;

use crate::live::LiveStream;

/// The stream this connection feeds. Dropping it without `release` still tells the live stream.
pub struct Publishing {
    pub live: Arc<LiveStream>,
    pub serial: u64,
    pub stream_id: u32,
    pub armed: bool,
}

impl Publishing {
    pub async fn release(mut self) {
        self.armed = false;
        self.live.dropped(self.serial).await;
    }
}

impl Drop for Publishing {
    fn drop(&mut self) {
        if self.armed
            && let Ok(rt) = tokio::runtime::Handle::try_current()
        {
            let (live, serial) = (self.live.clone(), self.serial);
            rt.spawn(async move { live.dropped(serial).await });
        }
    }
}

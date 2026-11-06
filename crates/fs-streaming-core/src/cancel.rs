//! A clonable "please stop", sent once and awaited by many.
//!
//! Pattern: Observer (a `watch` flag) — the core hands one to the relay with each check, so a `stop`
//! or a connecting sender can cancel the check from the moment its state is `checking`, with no race.

use tokio::sync::watch;

/// A signal that can be sent once and awaited by many.
#[derive(Clone)]
pub struct CancelToken(watch::Sender<bool>);

impl Default for CancelToken {
    fn default() -> Self {
        Self(watch::channel(false).0)
    }
}

impl std::fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CancelToken")
    }
}

impl CancelToken {
    /// Send the signal.
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }

    /// Wait for the signal.
    pub async fn cancelled(&self) {
        let mut rx = self.0.subscribe();
        let _ = rx.wait_for(|v| *v).await;
    }
}

//! Owned tasks and a cancel signal.
//!
//! Pattern: RAII / Guard — a [`Task`] aborts its task when dropped and can be stopped and awaited, so
//! nothing is left behind after stop, disable or delete. [`CancelToken`] is a clonable "please stop".

use std::future::Future;

use tokio::sync::watch;
use tokio::task::JoinHandle;

/// A spawned task that ends with its owner.
pub struct Task(Option<JoinHandle<()>>);

impl Task {
    /// Spawn.
    pub fn spawn<F: Future<Output = ()> + Send + 'static>(f: F) -> Self {
        Self(Some(tokio::spawn(f)))
    }

    /// Abort and wait until it is gone.
    pub async fn stop(mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
            let _ = h.await;
        }
    }

    /// Wait until it ends by itself.
    pub async fn join(mut self) {
        if let Some(h) = self.0.take() {
            let _ = h.await;
        }
    }

    /// Let it run on its own (used when a task ends itself).
    pub fn detach(mut self) {
        self.0.take();
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
        }
    }
}

/// A signal that can be sent once and awaited by many.
#[derive(Clone)]
pub struct CancelToken(watch::Sender<bool>);

impl Default for CancelToken {
    fn default() -> Self {
        Self(watch::channel(false).0)
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

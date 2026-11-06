//! Owned tasks and a cancel signal.
//!
//! Pattern: RAII / Guard — a [`Task`] aborts its task when dropped and can be stopped and awaited, so
//! nothing is left behind after stop, disable or delete. [`CancelToken`] is a clonable "please stop".

use std::future::Future;

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

    /// Wait for it to end by itself for up to `limit`; then cut it off and wait until it is gone.
    pub async fn join_for(mut self, limit: std::time::Duration) {
        if let Some(mut h) = self.0.take() {
            if tokio::time::timeout(limit, &mut h).await.is_err() {
                h.abort();
                let _ = h.await;
            }
        }
    }

    /// The task has ended.
    pub fn is_finished(&self) -> bool {
        self.0.as_ref().is_none_or(JoinHandle::is_finished)
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

pub use fs_streaming_core::CancelToken;

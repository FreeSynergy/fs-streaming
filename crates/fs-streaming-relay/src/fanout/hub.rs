//! `Hub`: the one place a sender's frames enter.
//!
//! Pattern: Observer — keeps the header cache, the flags `has_video`/`has_audio` and the list of
//! branches under one lock, so a branch that joins sees each frame exactly once from its start on.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use super::{Branch, Headers};
use crate::frame::{Frame, Kind};

#[derive(Default)]
struct Inner {
    headers: Headers,
    branches: Vec<Arc<Branch>>,
    has_video: bool,
    has_audio: bool,
}

/// Frames in, branches out.
#[derive(Default)]
pub struct Hub {
    inner: Mutex<Inner>,
    bytes_in: AtomicU64,
}

impl Hub {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Hand one frame to every branch.
    pub fn publish(&self, frame: Frame) {
        let frame = Arc::new(frame);
        let mut g = self.lock();
        match frame.kind {
            Kind::Video => g.has_video = true,
            Kind::Audio => g.has_audio = true,
            Kind::Data => {}
        }
        g.headers.update(&frame);
        let (heads, has_video) = (&g.headers, g.has_video);
        for b in &g.branches {
            b.offer(&frame, heads, has_video);
        }
    }

    /// Count bytes received from the sender.
    pub fn count_in(&self, n: usize) {
        self.bytes_in.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Bytes received so far.
    pub fn bytes_in(&self) -> u64 {
        self.bytes_in.load(Ordering::Relaxed)
    }

    /// `(has_video, has_audio)`.
    pub fn flags(&self) -> (bool, bool) {
        let g = self.lock();
        (g.has_video, g.has_audio)
    }

    /// Join: the branch gets frames from the next one on.
    pub fn add(&self, branch: Arc<Branch>) {
        self.lock().branches.push(branch);
    }

    /// Leave.
    pub fn remove(&self, branch: &Arc<Branch>) {
        self.lock().branches.retain(|b| !Arc::ptr_eq(b, branch));
    }
}

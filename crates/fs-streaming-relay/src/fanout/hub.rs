//! `Hub`: the one place a sender's frames enter.
//!
//! Pattern: Observer — keeps the header cache, the flags `has_video`/`has_audio` and the list of
//! branches under one lock, so a branch that joins sees each frame exactly once from its start on.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use super::{Branch, Headers};
use crate::frame::{Frame, Kind};

/// Keeps timestamps rising across senders: a new sender is shifted past the highest one given out.
#[derive(Default)]
struct Clock {
    last_out: u32,
    offset: u32,
    rebase: bool,
    seen: bool,
}

impl Clock {
    fn map(&mut self, ts: u32) -> u32 {
        if self.rebase {
            self.rebase = false;
            self.offset = if self.seen {
                self.last_out.saturating_add(1).saturating_sub(ts)
            } else {
                0
            };
        }
        let out = ts.wrapping_add(self.offset);
        self.last_out = self.last_out.max(out);
        self.seen = true;
        out
    }
}

#[derive(Default)]
struct Inner {
    headers: Headers,
    branches: Vec<Arc<Branch>>,
    has_video: bool,
    has_audio: bool,
    clock: Clock,
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
    pub fn publish(&self, mut frame: Frame) {
        let mut g = self.lock();
        frame.timestamp = g.clock.map(frame.timestamp);
        let frame = Arc::new(frame);
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

    /// A new sender begins: its timestamps are shifted so they keep rising for everything downstream.
    pub fn new_sender(&self) {
        self.lock().clock.rebase = true;
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

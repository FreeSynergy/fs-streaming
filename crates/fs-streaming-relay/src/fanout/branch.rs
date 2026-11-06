//! `Branch`: the bounded queue of one target (or of the recording).
//!
//! Pattern: State (open, skipping) + Producer/Consumer — the queue is limited in bytes. When it is
//! full the branch drops frames, counts them, and stays closed until the next keyframe arrives and the
//! queue has drained to half; then it sends the codec headers again and goes on. A branch that joins
//! waits for a keyframe the same way. Audio waits with the video, so they stay together.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;

use super::Headers;
use crate::frame::{Frame, Kind};

/// Counters of a branch, readable at any time.
#[derive(Debug, Default)]
pub struct BranchStats {
    /// Bytes written out.
    pub bytes_out: AtomicU64,
    /// Frames left out because the branch could not keep up.
    pub dropped: AtomicU64,
    /// Connections after the first.
    pub reconnects: AtomicU32,
}

#[derive(Default)]
struct Inner {
    queue: VecDeque<Arc<Frame>>,
    bytes: usize,
    open: bool,
    overflowed: bool,
    closed: bool,
    last_ts: u32,
}

/// One queue with its gate.
pub struct Branch {
    inner: Mutex<Inner>,
    notify: Notify,
    limit: usize,
    /// Counters.
    pub stats: BranchStats,
}

impl Branch {
    /// A closed branch with a limit in bytes.
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            notify: Notify::new(),
            limit,
            stats: BranchStats::default(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Offer a frame. Never blocks. `heads` already contains this frame if it is a header.
    pub fn offer(&self, frame: &Arc<Frame>, heads: &Headers, has_video: bool) {
        let mut g = self.lock();
        if g.closed {
            return;
        }
        if !g.open {
            let starts =
                frame.keyframe || (frame.kind == Kind::Audio && !has_video && !frame.header);
            let room = !g.overflowed || g.bytes <= self.limit / 2;
            if starts && room {
                g.open = true;
                g.overflowed = false;
                Self::clear(&mut g, &self.stats, true);
                // Resent headers must not take time backwards.
                let floor = g.last_ts;
                for h in heads.in_order() {
                    let mut h = (**h).clone();
                    h.timestamp = h.timestamp.max(floor);
                    Self::push(&mut g, Arc::new(h));
                }
                Self::push(&mut g, frame.clone());
                drop(g);
                self.notify.notify_one();
            } else if g.overflowed && frame.kind != Kind::Data && !frame.header {
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        if g.bytes + frame.payload.len() > self.limit {
            g.open = false;
            g.overflowed = true;
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        Self::push(&mut g, frame.clone());
        drop(g);
        self.notify.notify_one();
    }

    fn push(g: &mut Inner, f: Arc<Frame>) {
        g.last_ts = g.last_ts.max(f.timestamp);
        g.bytes += f.payload.len();
        g.queue.push_back(f);
    }

    fn clear(g: &mut Inner, stats: &BranchStats, count: bool) {
        if count {
            let media = g.queue.iter().filter(|f| !f.header).count() as u64;
            stats.dropped.fetch_add(media, Ordering::Relaxed);
        }
        g.queue.clear();
        g.bytes = 0;
    }

    /// Wait for the next frame; `None` once closed.
    pub async fn next(&self) -> Option<Arc<Frame>> {
        loop {
            {
                let mut g = self.lock();
                if g.closed && g.queue.is_empty() {
                    return None;
                }
                if let Some(f) = g.queue.pop_front() {
                    g.bytes -= f.payload.len();
                    return Some(f);
                }
            }
            self.notify.notified().await;
        }
    }

    /// Start over for a new connection: empty queue, gate closed (headers and a keyframe come first).
    pub fn reset(&self) {
        let mut g = self.lock();
        Self::clear(&mut g, &self.stats, false);
        g.open = false;
        g.overflowed = false;
        g.last_ts = 0;
    }

    /// No more frames; what is queued can still be read.
    pub fn close(&self) {
        self.lock().closed = true;
        self.notify.notify_one();
    }
}

#[cfg(test)]
#[path = "branch_tests.rs"]
mod tests;

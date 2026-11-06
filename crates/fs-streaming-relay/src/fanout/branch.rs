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
                for h in heads.in_order() {
                    Self::push(&mut g, h.clone());
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
    }

    /// No more frames; what is queued can still be read.
    pub fn close(&self) {
        self.lock().closed = true;
        self.notify.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn key() -> Arc<Frame> {
        Arc::new(Frame::new(
            Kind::Video,
            0,
            Bytes::from(vec![0x17, 1, 0, 0, 0, 1, 2, 3]),
        ))
    }
    fn inter(n: usize) -> Arc<Frame> {
        Arc::new(Frame::new(Kind::Video, 1, Bytes::from(vec![0x27; n])))
    }

    #[test]
    fn waits_for_a_keyframe_then_sends_headers_first() {
        let b = Branch::new(1000);
        let mut heads = Headers::default();
        let h = Arc::new(Frame::new(
            Kind::Video,
            0,
            Bytes::from_static(&[0x17, 0, 0, 0, 0]),
        ));
        heads.update(&h);
        b.offer(&inter(10), &heads, true);
        assert_eq!(b.lock().queue.len(), 0);
        assert_eq!(b.stats.dropped.load(Ordering::Relaxed), 0);
        b.offer(&key(), &heads, true);
        let q: Vec<_> = b.lock().queue.iter().map(|f| f.header).collect();
        assert_eq!(q, vec![true, false]);
    }

    #[test]
    fn overflow_drops_until_keyframe_with_room() {
        let b = Branch::new(100);
        let heads = Headers::default();
        b.offer(&key(), &heads, true);
        b.offer(&inter(60), &heads, true);
        b.offer(&inter(60), &heads, true);
        assert_eq!(b.stats.dropped.load(Ordering::Relaxed), 1);
        b.offer(&inter(5), &heads, true);
        assert_eq!(b.stats.dropped.load(Ordering::Relaxed), 2);
        b.offer(&key(), &heads, true);
        assert_eq!(
            b.stats.dropped.load(Ordering::Relaxed),
            3,
            "queue still above half: keyframe skipped too"
        );
        b.lock().queue.clear();
        b.lock().bytes = 0;
        b.offer(&key(), &heads, true);
        assert!(b.lock().open);
    }
}

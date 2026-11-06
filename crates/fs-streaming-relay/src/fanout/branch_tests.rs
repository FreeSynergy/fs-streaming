//! Tests of the branch gate: waiting for a keyframe, dropping on overflow.
//!
//! Pattern: plain unit tests — they look inside `Branch` to check the queue and the gate.

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

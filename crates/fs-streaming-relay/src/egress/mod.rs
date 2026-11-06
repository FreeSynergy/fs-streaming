//! Sending to one target.
//!
//! Pattern: Strategy-free worker loop — connect, publish, send the queue, and when the connection is
//! lost wait with growing pauses and start again at a keyframe.

mod backoff;
mod send;

pub use backoff::Backoff;
pub use send::{TargetCtx, run};

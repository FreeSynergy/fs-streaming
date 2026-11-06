//! The relay itself: listener, live streams, the `Relay` port.
//!
//! Pattern: Facade (declarations) — `RtmpRelay` is the one entry point the host creates.

mod check;
mod port;
mod rtmp;
mod shared;

pub use rtmp::RtmpRelay;
pub(crate) use shared::Shared;

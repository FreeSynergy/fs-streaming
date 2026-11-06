//! One running stream: its sender, hub, target workers and recorder.
//!
//! Pattern: Facade (declarations) — `LiveStream` owns every task of a stream, so ending it leaves none.

mod ops;
mod stream;
mod worker;

pub use stream::{LiveStream, Ticket};

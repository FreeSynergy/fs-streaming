//! The state machines: one transition function per stream and per target.
//!
//! Pattern: State — pure functions from (state, event) to the next state, so every rule of the
//! contract's header can be tested without a store or a socket.

mod stream;
mod target;

pub use stream::{Refusal, StreamEvent, next_stream};
pub use target::{TargetEvent, next_target};

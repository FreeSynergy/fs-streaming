//! Fan-out: one sender, many bounded queues.
//!
//! Pattern: Observer (publish/subscribe) — the hub offers every frame to each branch; a branch never
//! blocks the hub, so a slow target cannot slow the sender or the others.

mod branch;
mod headers;
mod hub;

pub use branch::Branch;
pub use headers::Headers;
pub use hub::Hub;

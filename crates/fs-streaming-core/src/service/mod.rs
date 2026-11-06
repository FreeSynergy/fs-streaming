//! The service: the core's one entry point.
//!
//! Pattern: Facade (declarations) — `StreamService` implements `Streams`, `Streaming` and `Gate`; one
//! file per concern: handle and views, runtime state, streams, targets, check, gate, capabilities.

mod caps;
mod check;
mod gate;
mod handle;
mod runtime;
mod status;
mod streams;
mod targets;

pub use handle::StreamService;

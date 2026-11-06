//! `fs-streaming-relay`: RTMP in, many RTMP and RTMPS targets out, recording as FLV.
//!
//! Frames are forwarded without decoding, payload untouched and timestamps kept. See `DESIGN.md`.
//!
//! Pattern: Facade — this file only declares modules and re-exports the public API.

#![deny(missing_docs)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::module_name_repetitions
)]

mod config;
mod egress;
mod error;
mod fanout;
mod frame;
mod ingest;
mod live;
mod record;
mod relay;
mod task;
mod wire;

pub use config::{Limits, RelayConfig};
pub use error::WireError;
pub use record::FolderSink;
pub use relay::RtmpRelay;

//! Recording: FLV files from the forwarded frames.
//!
//! Pattern: Strategy (the sink) + Observer (the recorder is one more branch of the hub).

mod flv;
mod folder;
mod recorder;

pub use flv::{HEADER as FLV_HEADER, tag as flv_tag};
pub use folder::FolderSink;
pub(crate) use recorder::Recorder;

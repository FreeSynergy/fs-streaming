//! The data types of the contract, in Rust form.
//!
//! Pattern: Facade (declarations) — one file per concept: stream, target, status, check, recording.

mod check;
mod page;
mod recording;
mod status;
mod stream;
mod target;

pub use check::{CheckResult, KeyCheck};
pub use page::{Cursor, Page};
pub use recording::{RecordingInfo, RecordingStart};
pub use status::{Status, TargetStatus};
pub use stream::{Created, Stream, StreamState};
pub use target::{Target, TargetState};

//! The port for recordings.
//!
//! Pattern: Strategy — `RecordingSink::create` gives a writer, `finish` closes it. A folder is one
//! sink; handing over to fs-files will be another. A failing sink never stops a stream.

use async_trait::async_trait;

use crate::model::{RecordingInfo, RecordingStart};

/// A sink failed. The wording is fixed; the cause stays out of it.
#[derive(Debug, thiserror::Error)]
#[error("the recording sink failed")]
pub struct SinkError;

/// Where recordings go.
#[async_trait]
pub trait RecordingSink: Send + Sync {
    /// Open a new recording.
    async fn create(&self, start: &RecordingStart) -> Result<Box<dyn RecordingWriter>, SinkError>;
}

/// One open recording.
#[async_trait]
pub trait RecordingWriter: Send {
    /// Append bytes.
    async fn write(&mut self, bytes: &[u8]) -> Result<(), SinkError>;
    /// Close it and say what it is.
    async fn finish(self: Box<Self>) -> Result<RecordingInfo, SinkError>;
}

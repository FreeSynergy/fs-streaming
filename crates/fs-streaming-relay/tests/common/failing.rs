//! A recording sink that fails, for tests.

use async_trait::async_trait;
use fs_streaming_core::{RecordingInfo, RecordingSink, RecordingStart, RecordingWriter, SinkError};

/// A sink that fails: at once, or after some bytes.
pub struct Failing {
    pub at_create: bool,
    pub after_bytes: usize,
}

struct FailingWriter {
    seen: usize,
    limit: usize,
}

#[async_trait]
impl RecordingSink for Failing {
    async fn create(&self, _: &RecordingStart) -> Result<Box<dyn RecordingWriter>, SinkError> {
        if self.at_create {
            return Err(SinkError);
        }
        Ok(Box::new(FailingWriter {
            seen: 0,
            limit: self.after_bytes,
        }))
    }
}

#[async_trait]
impl RecordingWriter for FailingWriter {
    async fn write(&mut self, bytes: &[u8]) -> Result<(), SinkError> {
        self.seen += bytes.len();
        if self.seen > self.limit {
            Err(SinkError)
        } else {
            Ok(())
        }
    }
    async fn finish(self: Box<Self>) -> Result<RecordingInfo, SinkError> {
        Err(SinkError)
    }
}

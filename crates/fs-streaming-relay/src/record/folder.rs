//! `FolderSink`: recordings as FLV files in a folder the host gives.
//!
//! Pattern: Strategy (a `RecordingSink`) — names are generated here (stream id, time, a counter), never
//! taken from outside, so a stream name cannot make a path. Handing over to fs-files will be another sink.

use std::path::PathBuf;

use async_trait::async_trait;
use fs_streaming_core::{RecordingInfo, RecordingSink, RecordingStart, RecordingWriter, SinkError};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

/// Writes `stream-<id>-<yyyymmdd-hhmmss>[-n].flv` into a folder.
pub struct FolderSink {
    dir: PathBuf,
}

impl FolderSink {
    /// A sink for `dir`; it is created when the first recording starts.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

struct FolderWriter {
    file: BufWriter<File>,
    path: PathBuf,
    start: RecordingStart,
    bytes: u64,
}

#[async_trait]
impl RecordingSink for FolderSink {
    async fn create(&self, start: &RecordingStart) -> Result<Box<dyn RecordingWriter>, SinkError> {
        tokio::fs::create_dir_all(&self.dir)
            .await
            .map_err(|_| SinkError)?;
        let stamp = start.from.format("%Y%m%d-%H%M%S");
        for n in 0..1000u32 {
            let suffix = if n == 0 {
                String::new()
            } else {
                format!("-{n}")
            };
            let path = self
                .dir
                .join(format!("stream-{}-{stamp}{suffix}.flv", start.stream));
            match tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await
            {
                Ok(f) => {
                    return Ok(Box::new(FolderWriter {
                        file: BufWriter::new(f),
                        path,
                        start: start.clone(),
                        bytes: 0,
                    }));
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(SinkError),
            }
        }
        Err(SinkError)
    }
}

#[async_trait]
impl RecordingWriter for FolderWriter {
    async fn write(&mut self, bytes: &[u8]) -> Result<(), SinkError> {
        self.file.write_all(bytes).await.map_err(|_| SinkError)?;
        self.bytes += bytes.len() as u64;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<RecordingInfo, SinkError> {
        self.file.flush().await.map_err(|_| SinkError)?;
        self.file.shutdown().await.map_err(|_| SinkError)?;
        Ok(RecordingInfo {
            stream: self.start.stream,
            name: self.start.name.clone(),
            file: self.path.to_string_lossy().into_owned(),
            file_id: None,
            bytes: self.bytes,
            from: self.start.from,
            to: chrono::Utc::now(),
        })
    }
}

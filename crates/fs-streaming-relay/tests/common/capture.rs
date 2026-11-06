//! Capturing what a program prints: `tracing` output and the feed.

use std::io::Write;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use fs_streaming_core::{ObjectId, StatusListener, Stream, Target};

#[derive(Clone, Default)]
pub struct Capture(pub Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Capture {
        self.clone()
    }
}

#[derive(Default)]
pub struct Feed(pub Mutex<Vec<String>>);

#[async_trait]
impl StatusListener for Feed {
    async fn stream(&self, s: Stream) {
        self.0.lock().unwrap().push(format!("{s:?}"));
    }
    async fn target(&self, t: Target) {
        self.0.lock().unwrap().push(format!("{t:?}"));
    }
    async fn recording(&self, r: fs_streaming_core::RecordingInfo) {
        self.0.lock().unwrap().push(format!("{r:?}"));
    }
    async fn removed(&self, id: ObjectId) {
        self.0.lock().unwrap().push(format!("{id:?}"));
    }
    async fn done(&self) {}
}

pub fn files_under(dir: &std::path::Path, out: &mut Vec<Vec<u8>>) {
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = e.path();
        if p.is_dir() {
            files_under(&p, out);
        } else if let Ok(b) = std::fs::read(&p) {
            out.push(b);
        }
    }
}

pub fn contains(hay: &[u8], needle: &str) -> bool {
    hay.windows(needle.len()).any(|w| w == needle.as_bytes())
}

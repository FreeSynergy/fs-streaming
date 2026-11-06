//! Operations on a running stream: recording, targets, counters.
//!
//! Pattern: Command-like methods on `LiveStream` — each takes the stream's one lock, so a change never
//! races with the end of the stream.

use fs_streaming_core::{
    Note, ObjectId, RecordingEvent, RecordingStart, RelayStatus, RelayTargetStatus, TargetSpec,
};

use super::stream::{Inner, LiveStream};
use super::worker::Worker;
use crate::record::Recorder;

impl LiveStream {
    pub(super) async fn start_recorder(&self, g: &mut Inner) {
        let Some(sink) = self.shared.cfg.sink.clone() else {
            self.shared
                .gate
                .recording_event(
                    self.id,
                    RecordingEvent::Problem(Some(Note::RecordingFailed)),
                )
                .await;
            return;
        };
        let start = RecordingStart {
            stream: self.id,
            name: self.name.clone(),
            from: chrono::Utc::now(),
        };
        g.recorder = Some(Recorder::start(
            sink,
            self.shared.gate.clone(),
            self.hub.clone(),
            start,
            self.shared.cfg.record_queue_bytes,
        ));
    }

    /// Start or stop the recording.
    pub async fn set_recording(&self, on: bool) {
        let mut g = self.inner.lock().await;
        if g.ended || !g.running {
            return;
        }
        match (on, g.recorder.take()) {
            (true, None) => self.start_recorder(&mut g).await,
            (true, Some(r)) => g.recorder = Some(r),
            (false, Some(r)) => r.finish().await,
            (false, None) => {}
        }
    }

    /// Add, change or remove a target of the running stream.
    pub async fn apply_target(&self, spec: TargetSpec, enabled: bool) {
        let mut g = self.inner.lock().await;
        if g.ended || !g.running {
            return;
        }
        let old = g.workers.remove(&spec.id);
        match (old, enabled) {
            (Some(w), false) => w.stop().await,
            (None, false) => {}
            (Some(w), true) if w.url == spec.url && !w.is_finished() => {
                w.set_spec(spec.clone());
                g.workers.insert(spec.id, w);
            }
            (old, true) => {
                if let Some(w) = old {
                    w.stop().await;
                }
                g.workers
                    .insert(spec.id, Worker::start(spec, self.hub.clone(), &self.shared));
            }
        }
    }

    /// Remove a target if this stream has it.
    pub async fn drop_target(&self, id: ObjectId) {
        let w = self.inner.lock().await.workers.remove(&id);
        if let Some(w) = w {
            w.stop().await;
        }
    }

    /// Counters.
    pub async fn status(&self) -> RelayStatus {
        let g = self.inner.lock().await;
        let (has_video, has_audio) = self.hub.flags();
        let targets = g.workers.values().map(|w| {
            let s = &w.branch.stats;
            let load =
                |a: &std::sync::atomic::AtomicU64| a.load(std::sync::atomic::Ordering::Relaxed);
            (
                w.id,
                RelayTargetStatus {
                    bytes_out: load(&s.bytes_out),
                    dropped: load(&s.dropped),
                    reconnects: s.reconnects.load(std::sync::atomic::Ordering::Relaxed),
                },
            )
        });
        RelayStatus {
            bytes_in: self.hub.bytes_in(),
            has_video,
            has_audio,
            targets: targets.collect(),
        }
    }
}

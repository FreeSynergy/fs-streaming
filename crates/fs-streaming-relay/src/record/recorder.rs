//! `Recorder`: one more branch of the hub, written through a sink.
//!
//! Pattern: Observer + Strategy — a failing sink sets a problem and ends the recording; the stream is
//! never touched. Starting mid-stream begins with the codec headers and a keyframe, like any joiner.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use fs_streaming_core::{Gate, Note, ObjectId, RecordingEvent, RecordingSink, RecordingStart};

use super::{FLV_HEADER, flv_tag};
use crate::fanout::{Branch, Hub};
use crate::task::Task;

/// A running recording.
pub struct Recorder {
    branch: Arc<Branch>,
    hub: Arc<Hub>,
    task: Task,
}

impl Recorder {
    /// Start recording the frames of `hub`.
    pub fn start(
        sink: Arc<dyn RecordingSink>,
        gate: Arc<dyn Gate>,
        hub: Arc<Hub>,
        start: RecordingStart,
        limit: usize,
    ) -> Self {
        let branch = Branch::new(limit);
        hub.add(branch.clone());
        let task = Task::spawn(run(sink, gate, hub.clone(), branch.clone(), start));
        Self { branch, hub, task }
    }

    /// Stop: write what is queued, close the file, report it.
    pub async fn finish(self) {
        self.hub.remove(&self.branch);
        self.branch.close();
        // The task ends itself once the queue is drained; a stuck sink is cut off after a while.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), self.task.join()).await;
    }
}

async fn run(
    sink: Arc<dyn RecordingSink>,
    gate: Arc<dyn Gate>,
    hub: Arc<Hub>,
    branch: Arc<Branch>,
    start: RecordingStart,
) {
    let stream: ObjectId = start.stream;
    let fail = |note: Note| {
        let (gate, hub, branch) = (gate.clone(), hub.clone(), branch.clone());
        async move {
            hub.remove(&branch);
            branch.close();
            gate.recording_event(stream, RecordingEvent::Problem(Some(note)))
                .await;
        }
    };
    let Ok(mut writer) = sink.create(&start).await else {
        return fail(Note::RecordingFailed).await;
    };
    gate.recording_event(stream, RecordingEvent::Problem(None))
        .await;
    if writer.write(&FLV_HEADER).await.is_err() {
        return fail(Note::RecordingFailed).await;
    }
    let mut slow = false;
    while let Some(frame) = branch.next().await {
        if writer.write(&flv_tag(&frame)).await.is_err() {
            return fail(Note::RecordingFailed).await;
        }
        if !slow && branch.stats.dropped.load(Ordering::Relaxed) > 0 {
            slow = true;
            gate.recording_event(
                stream,
                RecordingEvent::Problem(Some(Note::RecordingTooSlow)),
            )
            .await;
        }
    }
    match writer.finish().await {
        Ok(info) => {
            gate.recording_event(stream, RecordingEvent::Finished(info))
                .await;
        }
        Err(_) => fail(Note::RecordingFailed).await,
    }
}

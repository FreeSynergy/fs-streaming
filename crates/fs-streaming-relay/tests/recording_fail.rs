//! A recording that failed keeps its problem until it works again; a stuck sink is cut off.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use common::raw::{Answer, Raw};
use common::rig::Rig;
use common::wait::{LONG, until};
use fs_streaming_core::{
    Note, RecordingInfo, RecordingSink, RecordingStart, RecordingWriter, SinkError, Streams,
};
use fs_streaming_relay::{Limits, RelayConfig};

/// Fails to create the first time; later recordings work.
struct Flaky(AtomicUsize);

/// Works at first, then never finishes a write.
struct Stuck;

struct Nowhere;
struct Hangs;

#[async_trait]
impl RecordingSink for Flaky {
    async fn create(&self, _: &RecordingStart) -> Result<Box<dyn RecordingWriter>, SinkError> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(SinkError)
        } else {
            Ok(Box::new(Nowhere))
        }
    }
}

#[async_trait]
impl RecordingWriter for Nowhere {
    async fn write(&mut self, _: &[u8]) -> Result<(), SinkError> {
        Ok(())
    }
    async fn finish(self: Box<Self>) -> Result<RecordingInfo, SinkError> {
        Err(SinkError)
    }
}

#[async_trait]
impl RecordingSink for Stuck {
    async fn create(&self, _: &RecordingStart) -> Result<Box<dyn RecordingWriter>, SinkError> {
        Ok(Box::new(Hangs))
    }
}

#[async_trait]
impl RecordingWriter for Hangs {
    async fn write(&mut self, _: &[u8]) -> Result<(), SinkError> {
        std::future::pending().await
    }
    async fn finish(self: Box<Self>) -> Result<RecordingInfo, SinkError> {
        std::future::pending().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_problem_stays_when_a_sender_returns_and_set_recording_starts_a_new_recorder() {
    let sink = Arc::new(Flaky(AtomicUsize::new(0)));
    let rig = Rig::with(RelayConfig::new().sink(sink.clone())).await;
    let made = rig.stream("s").await;
    rig.service.set_recording(made.id, true).await.unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    let problem = || async { rig.service.status(made.id).await.unwrap().recording_problem };
    until("the problem is set", LONG, problem).await;
    drop(raw);
    until("interrupted", LONG, || async {
        (rig.service.get(made.id).await.unwrap().unwrap().state
            == fs_streaming_core::StreamState::Interrupted)
            .then_some(())
    })
    .await;
    let mut again = Raw::connect(rig.addr).await;
    assert_eq!(again.publish(&app, &name).await, Answer::Accepted);
    assert_eq!(
        problem().await,
        Some(Note::RecordingFailed),
        "coming back does not hide it"
    );
    rig.service.set_recording(made.id, true).await.unwrap();
    until("the new recorder works", LONG, || async {
        problem().await.is_none().then_some(())
    })
    .await;
    assert_eq!(
        sink.0.load(Ordering::SeqCst),
        2,
        "a second recorder was made"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sink_that_hangs_is_cut_off_and_leaves_no_task() {
    let limits = Limits {
        recorder_finish: Duration::from_millis(300),
        ..Limits::default()
    };
    let rig = Rig::with(RelayConfig::new().sink(Arc::new(Stuck)).limits(limits)).await;
    let alive = || {
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let base = alive();
    let made = rig.stream("s").await;
    rig.service.set_recording(made.id, true).await.unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    raw.headers().await;
    raw.tick(0, 300).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = std::time::Instant::now();
    rig.service.stop(made.id).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "stop did not wait for the sink for ever"
    );
    drop(raw);
    until("no task left", LONG, || async {
        (alive() <= base).then_some(())
    })
    .await;
}

//! Recording: a failing sink does not stop the stream; starting mid-stream begins with the headers.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::failing::Failing;
use common::ff;
use common::raw::{Answer, Raw};
use common::rig::{Rig, scratch};
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{Note, StreamState, Streams, TargetState};
use fs_streaming_relay::RelayConfig;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_sink_sets_a_problem_and_the_stream_goes_on() {
    for (name, sink) in [
        (
            "at create",
            Failing {
                at_create: true,
                after_bytes: 0,
            },
        ),
        (
            "while writing",
            Failing {
                at_create: false,
                after_bytes: 600,
            },
        ),
    ] {
        let rig = Rig::with(RelayConfig::new().sink(Arc::new(sink))).await;
        let target = Sink::start(Mode::Accept).await;
        let made = rig.stream("s").await;
        rig.service.set_recording(made.id, true).await.unwrap();
        rig.service
            .add_target(made.id, "t", &target.url("live"), "k")
            .await
            .unwrap();
        let (app, key) = made.app_and_name();
        let mut raw = Raw::connect(rig.addr).await;
        assert_eq!(raw.publish(&app, &key).await, Answer::Accepted);
        until("sending", LONG, || async {
            (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending)
                .then_some(())
        })
        .await;
        raw.headers().await;
        for n in 0..20 {
            raw.tick(n, 400).await;
        }
        until(name, LONG, || async {
            rig.service.status(made.id).await.unwrap().recording_problem
        })
        .await;
        let problem = rig.service.status(made.id).await.unwrap().recording_problem;
        assert_eq!(problem, Some(Note::RecordingFailed), "{name}");
        for n in 20..30 {
            raw.tick(n, 400).await;
        }
        until("the target kept receiving", LONG, || async {
            (target.media_count() >= 60).then_some(())
        })
        .await;
        assert_eq!(
            rig.service.get(made.id).await.unwrap().unwrap().state,
            StreamState::Live,
            "{name}: the stream goes on"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recording_started_mid_stream_begins_with_the_headers_and_a_keyframe() {
    let dir = scratch("midrec");
    let rig = Rig::recording(&dir.0).await;
    let made = rig.stream("../../evil name").await;
    let (app, key) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &key).await, Answer::Accepted);
    raw.headers().await;
    for n in 0..13 {
        raw.tick(n, 300).await;
    }
    until("the relay has taken what was sent", LONG, || async {
        (rig.service.status(made.id).await.unwrap().bytes_in >= 13 * 306).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    rig.service.set_recording(made.id, true).await.unwrap();
    for n in 13..40 {
        raw.tick(n, 300).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    rig.service.set_recording(made.id, false).await.unwrap();
    let files: Vec<_> = until("one closed file", LONG, || async {
        let v: Vec<_> = std::fs::read_dir(&dir.0).unwrap().flatten().collect();
        Some(v).filter(|v| !v.is_empty())
    })
    .await;
    assert_eq!(files.len(), 1);
    assert!(
        files[0]
            .file_name()
            .to_string_lossy()
            .starts_with("stream-"),
        "a generated name, not the stream's"
    );
    let flv = std::fs::read(files[0].path()).unwrap();
    assert_eq!(
        &flv[..13],
        &[b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0]
    );
    let tags = common::flv::tags(&flv);
    let kinds: Vec<u8> = tags.iter().take(4).map(|t| t.0).collect();
    assert_eq!(
        kinds,
        [18, 9, 8, 9],
        "metadata, video header, audio header, then the first frame"
    );
    assert_eq!(tags[3].2, 0x17, "which is a keyframe");
    assert!(
        tags[3].1 >= 13 * 40,
        "and it is from after recording was switched on"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ffprobe_reads_a_recording_started_mid_stream() {
    if ff::skip() {
        return;
    }
    let dir = scratch("midrec-ff");
    let video = dir.0.join("src.flv");
    ff::make_video(&video, 6);
    let rig = Rig::recording(&dir.0.join("rec")).await;
    let made = rig.stream("s").await;
    let mut child = ff::spawn_publish(&video, &made.full_url());
    until("some media", LONG, || async {
        (rig.service.status(made.id).await.unwrap().bytes_in > 50_000).then_some(())
    })
    .await;
    rig.service.set_recording(made.id, true).await.unwrap();
    assert!(child.wait().await.unwrap().success());
    rig.service.stop(made.id).await.unwrap();
    let files: Vec<_> = until("recording", LONG, || async {
        let v: Vec<_> = std::fs::read_dir(dir.0.join("rec"))
            .map(|d| d.flatten().collect())
            .unwrap_or_default();
        Some(v).filter(|v: &Vec<std::fs::DirEntry>| !v.is_empty())
    })
    .await;
    let p = ff::probe(&files[0].path());
    assert_eq!(
        (p.video.as_deref(), p.audio.as_deref()),
        (Some("h264"), Some("aac"))
    );
    assert_eq!(ff::decode_errors(&files[0].path()), "");
}

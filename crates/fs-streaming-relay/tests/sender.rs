//! The sender: refusals, newest wins, interrupted and ended, stop, rotateKey.

mod common;

use std::time::Duration;

use common::raw::{Answer, Raw};
use common::rig::{Made, Rig, scratch};
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{ObjectId, StreamState, Streams, TargetState};
use fs_streaming_relay::RelayConfig;
use tokio::io::AsyncReadExt;

async fn state(rig: &Rig, id: ObjectId) -> StreamState {
    rig.service.get(id).await.unwrap().unwrap().state
}

async fn becomes(rig: &Rig, id: ObjectId, want: StreamState) {
    until(&format!("state {want:?}"), LONG, || async {
        (state(rig, id).await == want).then_some(())
    })
    .await;
}

async fn publish(rig: &Rig, made: &Made) -> (Raw, Answer) {
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    let a = raw.publish(&app, &name).await;
    (raw, a)
}

async fn is_closed(raw: &mut Raw) -> bool {
    let mut b = [0u8; 4096];
    loop {
        match tokio::time::timeout(Duration::from_secs(10), raw.io.read(&mut b)).await {
            Ok(Ok(0) | Err(_)) => return true,
            Ok(Ok(_)) => {}
            Err(_) => return false,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_key_and_an_unknown_stream_get_the_same_refusal() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    let mut wrong = made.clone();
    wrong.key = "0".repeat(64);
    let mut unknown = made.clone();
    unknown.id = ObjectId::new();
    let (_w, a) = publish(&rig, &wrong).await;
    let (_u, b) = publish(&rig, &unknown).await;
    let (_keep, c) = publish(&rig, &made).await;
    assert_eq!(a, b, "the same answer for both");
    assert!(matches!(a, Answer::Refused(_)));
    assert_eq!(c, Answer::Accepted);
    assert_eq!(
        state(&rig, made.id).await,
        StreamState::Live,
        "the refusals changed nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_newest_sender_wins_and_the_stream_stays_live() {
    let rig = Rig::new().await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &sink.url("live"), "k")
        .await
        .unwrap();
    let (mut old, a) = publish(&rig, &made).await;
    assert_eq!(a, Answer::Accepted);
    let (mut new, b) = publish(&rig, &made).await;
    assert_eq!(
        b,
        Answer::Accepted,
        "the newest one with the right key is taken"
    );
    assert!(is_closed(&mut old).await, "the older sender was ended");
    assert_eq!(state(&rig, made.id).await, StreamState::Live);
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    new.headers().await;
    new.tick(0, 500).await;
    until("the new sender's frames arrive", LONG, || async {
        (sink.media_count() >= 4).then_some(())
    })
    .await;
    assert_eq!(
        sink.accepts.lock().unwrap().len(),
        1,
        "the target stayed connected"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_short_drop_is_interrupted_and_continues_a_long_one_ends() {
    let dir = scratch("interrupt");
    let cfg = RelayConfig::new()
        .sink(std::sync::Arc::new(fs_streaming_relay::FolderSink::new(
            &dir.0,
        )))
        .grace_for_tests(Duration::from_millis(1500));
    let rig = Rig::with(cfg).await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("s").await;
    rig.service.set_recording(made.id, true).await.unwrap();
    rig.service
        .add_target(made.id, "t", &sink.url("live"), "k")
        .await
        .unwrap();
    let (mut raw, _) = publish(&rig, &made).await;
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    raw.headers().await;
    raw.tick(0, 500).await;
    raw.tick(1, 500).await;
    drop(raw);
    becomes(&rig, made.id, StreamState::Interrupted).await;
    assert_eq!(
        rig.service.targets(made.id).await.unwrap()[0].state,
        TargetState::Sending,
        "targets stay connected"
    );
    let (mut raw, a) = publish(&rig, &made).await;
    assert_eq!(a, Answer::Accepted, "a sender coming back continues");
    becomes(&rig, made.id, StreamState::Live).await;
    raw.headers().await;
    raw.tick(10, 500).await;
    raw.tick(11, 500).await;
    until("frames from both parts", LONG, || async {
        sink.messages()
            .iter()
            .any(|g| g.type_id == 9 && g.ts == 440)
            .then_some(())
    })
    .await;
    assert_eq!(
        sink.accepts.lock().unwrap().len(),
        1,
        "the target was not reconnected"
    );
    drop(raw);
    becomes(&rig, made.id, StreamState::Interrupted).await;
    becomes(&rig, made.id, StreamState::Ended).await;
    until("the target waits", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Waiting).then_some(())
    })
    .await;
    let files: Vec<_> = until("one closed recording", LONG, || async {
        let v: Vec<_> = std::fs::read_dir(&dir.0).unwrap().flatten().collect();
        Some(v).filter(|v| !v.is_empty())
    })
    .await;
    assert_eq!(
        files.len(),
        1,
        "the recording is one file across the interruption"
    );
    let tags = common::flv::tags(&std::fs::read(files[0].path()).unwrap());
    for ts in [0, 40, 400, 440] {
        assert!(
            tags.iter().any(|t| t.0 == 9 && t.1 == ts),
            "video at {ts} is in the one file"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_ends_the_sender_which_can_reconnect_and_rotate_key_keeps_it_out() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    let (mut raw, _) = publish(&rig, &made).await;
    becomes(&rig, made.id, StreamState::Live).await;
    let stopped = rig.service.stop(made.id).await.unwrap();
    assert!(is_closed(&mut raw).await, "stop ended the sender");
    assert_eq!(stopped.state, StreamState::Ended);
    let (mut again, a) = publish(&rig, &made).await;
    assert_eq!(a, Answer::Accepted, "it can connect again after stop");
    becomes(&rig, made.id, StreamState::Live).await;
    let new_key = rig.service.rotate_key(made.id).await.unwrap();
    assert!(
        is_closed(&mut again).await,
        "rotateKey ended the running sender"
    );
    assert!(
        matches!(publish(&rig, &made).await.1, Answer::Refused(_)),
        "the old key stays out"
    );
    let mut fresh = made.clone();
    fresh.key = new_key.expose().to_string();
    assert_eq!(publish(&rig, &fresh).await.1, Answer::Accepted);
}

//! A slow or dead target never slows the sender or the other targets.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::raw::{Answer, Raw};
use common::rig::Rig;
use common::sink::{Got, Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{ObjectId, Streams, TargetState};
use fs_streaming_relay::RelayConfig;

async fn both_sending(rig: &Rig, id: ObjectId, n: usize) {
    until("the targets are sending", LONG, || async {
        let t = rig.service.targets(id).await.unwrap();
        (t.iter().filter(|t| t.state == TargetState::Sending).count() >= n).then_some(())
    })
    .await;
}

/// Media messages that are not headers: (type, timestamp, first byte).
fn media(msgs: &[Got]) -> Vec<(u8, u32, u8)> {
    msgs.iter()
        .enumerate()
        .filter(|(_, g)| !is_header(g))
        .map(|(_, g)| (g.type_id, g.ts, g.data[0]))
        .collect()
}

fn is_header(g: &Got) -> bool {
    g.type_id == 18 || (g.type_id == 9 && g.data[1] == 0) || (g.type_id == 8 && g.data[1] == 0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_that_stops_reading_drops_frames_and_rejoins_at_a_keyframe() {
    let rig = Rig::with(RelayConfig::new().queue_bytes(150_000)).await;
    let (slow, fast) = (
        Sink::start(Mode::Stall).await,
        Sink::start(Mode::Accept).await,
    );
    let made = rig.stream("slow").await;
    let t_slow = rig
        .service
        .add_target(made.id, "slow", &slow.url("live"), "k")
        .await
        .unwrap();
    rig.service
        .add_target(made.id, "fast", &fast.url("live"), "k")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    both_sending(&rig, made.id, 2).await;
    raw.headers().await;
    let started = std::time::Instant::now();
    for n in 0..300 {
        raw.tick(n, 8_000).await;
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let sending_took = started.elapsed();
    let dropped = until("frames are dropped for the slow target", LONG, || async {
        let s = rig.service.status(made.id).await.unwrap();
        s.targets
            .iter()
            .find(|t| t.target == t_slow.id)
            .map(|t| t.dropped)
            .filter(|d| *d > 0)
    })
    .await;
    assert!(dropped > 0);
    until("the fast target has everything", LONG, || async {
        (fast.media_count() >= 600).then_some(())
    })
    .await;
    assert!(
        sending_took < Duration::from_secs(20),
        "the sender was not held back: {sending_took:?}"
    );
    let fast_media = media(&fast.messages());
    assert_eq!(fast_media.len(), 600, "the fast target lost nothing");
    slow.release();
    for n in 300..360 {
        raw.tick(n, 8_000).await;
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let last = 359 * 40;
    until("the slow target caught up", LONG, || async {
        media(&slow.messages())
            .iter()
            .any(|m| m.1 == last)
            .then_some(())
    })
    .await;
    let msgs = slow.messages();
    let mut rejoins = 0;
    let mut prev_video = None;
    let video_ts: Vec<u32> = media(&msgs)
        .iter()
        .filter(|m| m.0 == 9)
        .map(|m| m.1)
        .collect();
    for (i, g) in msgs
        .iter()
        .enumerate()
        .filter(|(_, g)| !is_header(g) && g.type_id == 9)
    {
        if prev_video.is_some_and(|p| g.ts > p + 40) {
            rejoins += 1;
            assert_eq!(g.data[0], 0x17, "a rejoin starts at a keyframe");
            assert!(
                i >= 3 && msgs[i - 3..i].iter().all(is_header),
                "the codec headers come first"
            );
            assert_eq!(
                msgs[i - 3..i].iter().map(|m| m.type_id).collect::<Vec<_>>(),
                [18, 9, 8]
            );
        }
        prev_video = Some(g.ts);
    }
    assert!(rejoins >= 1, "the slow target rejoined at least once");
    let audio_alone = media(&msgs)
        .iter()
        .filter(|m| m.0 == 8)
        .any(|m| !video_ts.contains(&m.1));
    assert!(!audio_alone, "audio is dropped together with its video");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_target_retries_with_growing_pauses_and_the_others_go_on() {
    let cfg = RelayConfig::new().retry(Duration::from_millis(250), Duration::from_secs(5));
    let rig = Rig::with(cfg).await;
    let (dead, fast) = (
        Sink::start(Mode::HangUp).await,
        Arc::new(Sink::start(Mode::Accept).await),
    );
    let made = rig.stream("dead").await;
    let t_dead = rig
        .service
        .add_target(made.id, "dead", &dead.url("live"), "k")
        .await
        .unwrap();
    rig.service
        .add_target(made.id, "fast", &fast.url("live"), "k")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    both_sending(&rig, made.id, 1).await;
    raw.headers().await;
    let mut n = 0;
    let started = std::time::Instant::now();
    while dead.accepts.lock().unwrap().len() < 5 {
        assert!(started.elapsed() < LONG, "the dead target was not retried");
        raw.tick(n, 3000).await;
        n += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let at = dead.accepts.lock().unwrap().clone();
    let gaps: Vec<_> = at.windows(2).map(|w| w[1] - w[0]).collect();
    assert!(
        gaps[1] > gaps[0] && gaps[2] > gaps[1],
        "the pauses grow: {gaps:?}"
    );
    let t = rig.service.targets(made.id).await.unwrap();
    let dead_now = t.iter().find(|x| x.id == t_dead.id).unwrap();
    assert_eq!(dead_now.state, TargetState::Retrying);
    assert!(dead_now.note.is_some());
    assert!(
        rig.service
            .status(made.id)
            .await
            .unwrap()
            .targets
            .iter()
            .any(|x| x.reconnects >= 3)
    );
    assert!(
        fast.media_count() >= 2 * (n as usize - 1),
        "the other target got every frame"
    );
}

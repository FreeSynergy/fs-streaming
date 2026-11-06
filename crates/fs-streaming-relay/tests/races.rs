//! State races between a sender coming back, the end of its grace time, and a rotated key.

mod common;

use std::time::Duration;

use common::raw::{Answer, Raw};
use common::rig::{Made, Rig};
use common::wait::{LONG, until};
use fs_streaming_core::{Error, Refusal, StreamState, Streams};
use fs_streaming_relay::RelayConfig;

async fn try_publish(rig: &Rig, made: &Made, key: &str) -> (Raw, Answer) {
    let mut raw = Raw::connect(rig.addr).await;
    let a = raw.publish(&format!("live/{}", made.id), key).await;
    (raw, a)
}

async fn state(rig: &Rig, made: &Made) -> StreamState {
    rig.service.get(made.id).await.unwrap().unwrap().state
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_coming_back_around_the_end_of_the_grace_time_is_never_left_ended() {
    let rig = Rig::with(RelayConfig::new().grace_for_tests(Duration::from_millis(40))).await;
    let made = rig.stream("s").await;
    for round in 0..30u64 {
        let (first, a) = try_publish(&rig, &made, &made.key).await;
        assert_eq!(a, Answer::Accepted);
        drop(first);
        tokio::time::sleep(Duration::from_millis(28 + round % 4 * 7)).await;
        let (second, b) = try_publish(&rig, &made, &made.key).await;
        assert_eq!(b, Answer::Accepted, "round {round}");
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            state(&rig, &made).await,
            StreamState::Live,
            "round {round}: alive means live"
        );
        let e = rig.service.delete(made.id).await.unwrap_err();
        assert!(
            matches!(e, Error::Refused(Refusal::StreamBusy)),
            "round {round}: {e}"
        );
        drop(second);
        until("ended", LONG, || async {
            (state(&rig, &made).await == StreamState::Ended).then_some(())
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_key_cannot_get_in_while_the_key_is_being_rotated() {
    let rig = Rig::new().await;
    for round in 0..25 {
        let made = rig.stream("s").await;
        let (r2, m2) = (rig.addr, made.clone());
        let old = tokio::spawn(async move {
            let mut raw = Raw::connect(r2).await;
            let a = raw.publish(&format!("live/{}", m2.id), &m2.key).await;
            (raw, a)
        });
        tokio::time::sleep(Duration::from_millis(round % 5)).await;
        rig.service.rotate_key(made.id).await.unwrap();
        let (_raw, _answer) = old.await.unwrap();
        until("the old key leaves nothing live", LONG, || async {
            (state(&rig, &made).await != StreamState::Live).then_some(())
        })
        .await;
        rig.service.stop(made.id).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_that_connects_around_stop_is_never_left_ended_while_alive() {
    use tokio::io::AsyncReadExt;
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    for round in 0..25u64 {
        let (first, a) = try_publish(&rig, &made, &made.key).await;
        assert_eq!(a, Answer::Accepted);
        let (addr, m2) = (rig.addr, made.clone());
        let back = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(round % 6)).await;
            let mut raw = Raw::connect(addr).await;
            let a = raw.publish(&format!("live/{}", m2.id), &m2.key).await;
            (raw, a)
        });
        rig.service.stop(made.id).await.unwrap();
        let (mut second, answer) = back.await.unwrap();
        drop(first);
        tokio::time::sleep(Duration::from_millis(250)).await;
        let mut b = [0u8; 1024];
        let alive = answer == Answer::Accepted
            && tokio::time::timeout(Duration::from_millis(200), second.io.read(&mut b))
                .await
                .is_err();
        if alive {
            assert_eq!(
                state(&rig, &made).await,
                StreamState::Live,
                "round {round}: alive means live"
            );
        }
        rig.service.stop(made.id).await.unwrap();
        until("ended", LONG, || async {
            (state(&rig, &made).await != StreamState::Live).then_some(())
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_takeover_the_old_senders_frames_are_dropped_and_time_keeps_rising() {
    use common::sink::{Mode, Sink};
    use fs_streaming_core::TargetState;
    let rig = Rig::new().await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &sink.url("live"), "k")
        .await
        .unwrap();
    let (mut old, _) = try_publish(&rig, &made, &made.key).await;
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    old.headers().await;
    let flood = tokio::spawn(async move {
        for n in 0..4000u32 {
            old.video(
                n * 40,
                &[if n % 10 == 0 { 0x17 } else { 0x27 }, 1, 0, 0, 0, 0xAA],
            )
            .await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    until("old frames flow", LONG, || async {
        (sink.media_count() > 20).then_some(())
    })
    .await;
    let (mut new, a) = try_publish(&rig, &made, &made.key).await;
    assert_eq!(a, Answer::Accepted);
    new.headers().await;
    for n in 0..40u32 {
        new.video(
            n * 40,
            &[if n % 10 == 0 { 0x17 } else { 0x27 }, 1, 0, 0, 0, 0xBB],
        )
        .await;
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    until("new frames arrive", LONG, || async {
        (sink
            .messages()
            .iter()
            .filter(|g| g.data.last() == Some(&0xBB))
            .count()
            >= 20)
            .then_some(())
    })
    .await;
    flood.abort();
    let got = sink.messages();
    let ts: Vec<u32> = got.iter().map(|g| g.ts).collect();
    assert!(
        ts.windows(2).all(|w| w[0] <= w[1]),
        "time never goes back: {ts:?}"
    );
    let first_new = got
        .iter()
        .position(|g| g.data.last() == Some(&0xBB))
        .unwrap();
    assert!(
        got[first_new..]
            .iter()
            .all(|g| g.data.last() != Some(&0xAA)),
        "nothing of the old sender after the takeover"
    );
}

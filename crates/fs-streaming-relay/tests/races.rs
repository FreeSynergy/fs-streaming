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

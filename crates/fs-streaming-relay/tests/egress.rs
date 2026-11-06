//! Sending to targets: pauses, explicit refusals at any time, servers that talk in their own way.

mod common;

use std::time::Duration;

use common::raw::{Answer, Raw};
use common::rig::Rig;
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{KeyCheck, Streams, TargetState};
use fs_streaming_relay::RelayConfig;

fn quick() -> RelayConfig {
    RelayConfig::new().retry(Duration::from_millis(200), Duration::from_secs(5))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_that_accepts_and_then_closes_gets_growing_pauses() {
    let rig = Rig::with(quick()).await;
    let flaky = Sink::start(Mode::DropAfterPublish).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &flaky.url("live"), "k")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    until("five tries", LONG, || async {
        (flaky.accepts.lock().unwrap().len() >= 5).then_some(())
    })
    .await;
    let at = flaky.accepts.lock().unwrap().clone();
    let gaps: Vec<_> = at.windows(2).map(|w| w[1] - w[0]).collect();
    assert!(
        gaps[2] > gaps[0] * 2,
        "accepting and closing is a loss, and the pause grows: {gaps:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_explicit_no_after_the_publish_was_accepted_is_refused_not_retried() {
    let rig = Rig::with(quick()).await;
    let later = Sink::start(Mode::RefuseLater).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &later.url("live"), "k")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    raw.headers().await;
    raw.tick(0, 300).await;
    until("refused", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Refused).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        later.accepts.lock().unwrap().len(),
        1,
        "no more tries after an explicit no"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_status_that_is_not_an_error_before_the_answer_to_connect_is_not_a_refusal() {
    let rig = Rig::new().await;
    let chatty = Sink::start(Mode::Chatty).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &chatty.url("live"), "k")
        .await
        .unwrap();
    let r = rig.service.check(made.id).await.unwrap();
    assert_eq!(
        (r[0].reached, r[0].key),
        (true, KeyCheck::Accepted),
        "{}",
        r[0].note_text()
    );
}

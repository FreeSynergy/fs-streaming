//! No task is left behind after stop, disable and delete.

mod common;

use std::time::Duration;

use common::raw::{Answer, Raw};
use common::rig::{Rig, scratch};
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{Streams, TargetState};

fn alive() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

async fn settle_to(what: &str, want: usize) {
    until(what, LONG, || async { (alive() == want).then_some(()) }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_disable_remove_and_delete_leave_no_task_behind() {
    let dir = scratch("tasks");
    let (one, two) = (Sink::isolated(Mode::Accept), Sink::isolated(Mode::Accept));
    let rig = Rig::recording(&dir.0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let base = alive();
    let made = rig.stream("s").await;
    rig.service.set_recording(made.id, true).await.unwrap();
    let a = rig
        .service
        .add_target(made.id, "a", &one.url("live"), "k")
        .await
        .unwrap();
    let b = rig
        .service
        .add_target(made.id, "b", &two.url("live"), "k")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    until("both sending", LONG, || async {
        let t = rig.service.targets(made.id).await.unwrap();
        t.iter()
            .all(|t| t.state == TargetState::Sending)
            .then_some(())
    })
    .await;
    raw.headers().await;
    raw.tick(0, 500).await;
    let running = alive();
    assert!(
        running >= base + 4,
        "a session, two workers and a recorder are running: {base} -> {running}"
    );

    rig.service.set_target_enabled(a.id, false).await.unwrap();
    settle_to("disable ended one worker", running - 1).await;
    rig.service.set_target_enabled(a.id, true).await.unwrap();
    until("enabled again", LONG, || async {
        (alive() == running).then_some(())
    })
    .await;
    rig.service.remove_target(b.id).await.unwrap();
    settle_to("remove ended a worker", running - 1).await;
    rig.service.set_recording(made.id, false).await.unwrap();
    settle_to("recording off ended the recorder", running - 2).await;

    rig.service.stop(made.id).await.unwrap();
    drop(raw);
    settle_to("stop left nothing", base).await;
    let mut again = Raw::connect(rig.addr).await;
    assert_eq!(again.publish(&app, &name).await, Answer::Accepted);
    until("live again", LONG, || async {
        (alive() > base).then_some(())
    })
    .await;
    rig.service.stop(made.id).await.unwrap();
    drop(again);
    settle_to("second stop left nothing", base).await;
    rig.service.delete(made.id).await.unwrap();
    settle_to("delete left nothing", base).await;
    rig.relay.shutdown().await;
    settle_to("shutdown ended the listener", base - 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_sender_after_its_time_leaves_no_task_behind() {
    let cfg = fs_streaming_relay::RelayConfig::new().grace_for_tests(Duration::from_millis(300));
    let sink = Sink::isolated(Mode::Accept);
    let rig = Rig::with(cfg).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let base = alive();
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "a", &sink.url("live"), "k")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    drop(raw);
    settle_to("interrupted, then ended, nothing left", base).await;
}

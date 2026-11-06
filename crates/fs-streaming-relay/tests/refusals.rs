//! The state machine seen from the service: each refusal of the contract.

mod common;

use common::raw::{Answer, Raw};
use common::rig::{Made, Rig};
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{Error, Refusal, StreamService, StreamState, Streams, TargetState};

async fn go_live(rig: &Rig, made: &Made) -> Raw {
    let (app, key) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &key).await, Answer::Accepted);
    raw
}

fn refused(e: Error) -> Refusal {
    match e {
        Error::Refused(r) => r,
        other => panic!("expected a refusal, got: {other}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_is_refused_while_live_interrupted_or_checking_and_then_works() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    let raw = go_live(&rig, &made).await;
    assert_eq!(
        refused(rig.service.delete(made.id).await.unwrap_err()),
        Refusal::StreamBusy
    );
    drop(raw);
    until("interrupted", LONG, || async {
        (rig.service.get(made.id).await.unwrap().unwrap().state == StreamState::Interrupted)
            .then_some(())
    })
    .await;
    assert_eq!(
        refused(rig.service.delete(made.id).await.unwrap_err()),
        Refusal::StreamBusy
    );
    assert!(
        rig.service.get(made.id).await.unwrap().is_some(),
        "nothing was deleted"
    );
    rig.service.stop(made.id).await.unwrap();
    let silent = Sink::start(Mode::Silent).await;
    let t = rig
        .service
        .add_target(made.id, "t", &silent.url("live"), "k")
        .await
        .unwrap();
    let (service, id) = (rig.service.clone(), made.id);
    let running = tokio::spawn(async move { service.check(id).await });
    until("checking", LONG, || async {
        (rig.service.get(made.id).await.unwrap().unwrap().state == StreamState::Checking)
            .then_some(())
    })
    .await;
    assert_eq!(
        refused(rig.service.delete(made.id).await.unwrap_err()),
        Refusal::StreamBusy
    );
    rig.service.stop(made.id).await.unwrap();
    running.await.unwrap().unwrap();
    rig.service.delete(made.id).await.unwrap();
    assert!(rig.service.get(made.id).await.unwrap().is_none());
    assert!(
        matches!(rig.service.targets(made.id).await, Err(Error::NotFound(_))),
        "its targets went with it"
    );
    assert!(matches!(
        rig.service.set_target_enabled(t.id, true).await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_that_is_sending_cannot_be_edited_but_a_key_and_a_switch_can_change() {
    let rig = Rig::new().await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("s").await;
    let t = rig
        .service
        .add_target(made.id, "t", &sink.url("live"), "k")
        .await
        .unwrap();
    assert_eq!(t.state, TargetState::Waiting, "the stream is not live");
    let _raw = go_live(&rig, &made).await;
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    let e = rig
        .service
        .edit_target(t.id, "new name", &sink.url("other"))
        .await
        .unwrap_err();
    assert_eq!(refused(e), Refusal::TargetSending);
    assert_eq!(
        rig.service.targets(made.id).await.unwrap()[0].name,
        "t",
        "nothing changed"
    );
    rig.service.set_target_key(t.id, "newer").await.unwrap();
    let off = rig.service.set_target_enabled(t.id, false).await.unwrap();
    assert_eq!(off.state, TargetState::Off);
    rig.service
        .edit_target(t.id, "new name", &sink.url("other"))
        .await
        .unwrap();
    assert_eq!(
        rig.service.targets(made.id).await.unwrap()[0].name,
        "new name"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_relay_nothing_runs_and_the_page_lists_streams() {
    let store = std::sync::Arc::new(fs_store::TursoStore::open_in_memory().await.unwrap());
    let bare = StreamService::new(store);
    assert!(matches!(bare.create("x").await, Err(Error::NoRelay)));
    let rig = Rig::new().await;
    for i in 0..3 {
        rig.stream(&format!("s{i}")).await;
    }
    let page = rig.service.list(None).await.unwrap();
    assert_eq!(page.items.len(), 3);
    assert!(page.next.is_none());
    assert_eq!(page.items[0].name, "s0");
}

//! `check`: tries the targets without sending media, and says only what it knows.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::raw::{Answer, Raw};
use common::rig::Rig;
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{Error, KeyCheck, Note, Refusal, StreamState, Streams};
use fs_streaming_relay::{Limits, RelayConfig};

fn quick() -> RelayConfig {
    let limits = Limits {
        handshake_timeout: Duration::from_millis(500),
        answer_timeout: Duration::from_millis(500),
        ..Limits::default()
    };
    RelayConfig::new().limits(limits)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accepting_server_gives_accepted_and_gets_no_media() {
    let rig = Rig::new().await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &sink.url("live"), "thekey")
        .await
        .unwrap();
    let r = rig.service.check(made.id).await.unwrap();
    assert_eq!(r.len(), 1);
    assert!(r[0].reached);
    assert_eq!(r[0].key, KeyCheck::Accepted);
    assert!(
        r[0].notes.contains(&Note::KeyTravelsReadable),
        "a plain rtmp key is warned about: {}",
        r[0].note_text()
    );
    assert_eq!(
        sink.publishes.lock().unwrap().len(),
        1,
        "a publish was tried"
    );
    assert_eq!(sink.messages().len(), 0, "and no media was sent");
    assert_eq!(
        rig.service.get(made.id).await.unwrap().unwrap().state,
        StreamState::Idle,
        "back to where it was"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_explicit_refusal_is_rejected_nothing_listening_is_not_reached() {
    let rig = Rig::new().await;
    let refuse = Sink::start(Mode::Refuse).await;
    let closed = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        format!("rtmp://{}/live", l.local_addr().unwrap())
    };
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "refuse", &refuse.url("live"), "k")
        .await
        .unwrap();
    rig.service
        .add_target(made.id, "nobody", &closed, "k")
        .await
        .unwrap();
    rig.service
        .add_target(made.id, "nokey", &refuse.url("live"), "")
        .await
        .unwrap();
    let r = rig.service.check(made.id).await.unwrap();
    assert_eq!((r[0].reached, r[0].key), (true, KeyCheck::Rejected));
    assert_eq!((r[1].reached, r[1].key), (false, KeyCheck::Unknown));
    assert_eq!(
        (r[2].reached, r[2].key),
        (true, KeyCheck::Unknown),
        "no key, so none was tried"
    );
    assert!(r[2].notes.contains(&Note::NoKeyToTry));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_that_says_nothing_times_out_and_is_unknown() {
    let rig = Rig::with(quick()).await;
    let silent = Sink::start(Mode::Silent).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "silent", &silent.url("live"), "k")
        .await
        .unwrap();
    let r = rig.service.check(made.id).await.unwrap();
    assert_eq!((r[0].reached, r[0].key), (true, KeyCheck::Unknown));
    assert_eq!(r[0].notes, vec![Note::NoAnswer]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_is_refused_while_live_and_while_checking() {
    let rig = Arc::new(Rig::new().await);
    let made = rig.stream("s").await;
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    let err = rig.service.check(made.id).await.unwrap_err();
    assert!(
        matches!(err, Error::Refused(Refusal::CheckWhileLive)),
        "{err}"
    );
    assert_eq!(
        rig.service.get(made.id).await.unwrap().unwrap().state,
        StreamState::Live,
        "the real sender is untouched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_and_a_connecting_sender_cancel_a_running_check() {
    let rig = Arc::new(Rig::new().await);
    let silent = Sink::start(Mode::Silent).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "silent", &silent.url("live"), "k")
        .await
        .unwrap();
    for by_stop in [true, false] {
        let (r, id) = (rig.clone(), made.id);
        let running = tokio::spawn(async move { r.service.check(id).await });
        until("checking", LONG, || async {
            (rig.service.get(id).await.unwrap().unwrap().state == StreamState::Checking)
                .then_some(())
        })
        .await;
        let again = rig.service.check(id).await.unwrap_err();
        assert!(matches!(again, Error::Refused(Refusal::CheckRunning)));
        let _keep;
        if by_stop {
            rig.service.stop(id).await.unwrap();
        } else {
            let (app, name) = made.app_and_name();
            let mut raw = Raw::connect(rig.addr).await;
            assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
            _keep = raw;
        }
        let results = tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .expect("the check ended at once")
            .unwrap()
            .unwrap();
        assert_eq!(results[0].notes, vec![Note::Cancelled]);
        let state = rig.service.get(id).await.unwrap().unwrap().state;
        assert_eq!(
            state,
            if by_stop {
                StreamState::Idle
            } else {
                StreamState::Live
            }
        );
        if !by_stop {
            rig.service.stop(id).await.unwrap();
        }
    }
}

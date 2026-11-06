//! rtmps: certificates are checked, the host name too, and there is no switch to turn that off.

mod common;

use common::raw::{Answer, Raw};
use common::rig::Rig;
use common::sink::{Mode, Sink};
use common::tls::pki;
use common::wait::{LONG, until};
use fs_streaming_core::{KeyCheck, Note, Streams, TargetState};
use fs_streaming_relay::RelayConfig;

async fn tls_sink(names: &[&str]) -> (Sink, Vec<u8>) {
    let p = pki(names);
    (
        Sink::start_with(Mode::Accept, Some(p.acceptor)).await,
        p.ca_der,
    )
}

fn rtmps(sink: &Sink, host: &str) -> String {
    format!("rtmps://{host}:{}/live", sink.addr.port())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_private_ca_through_the_test_hook_works_and_the_key_is_not_called_readable() {
    let (sink, ca) = tls_sink(&["localhost"]).await;
    let rig = Rig::with(RelayConfig::new().trust_root_for_tests(ca)).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &rtmps(&sink, "localhost"), "k")
        .await
        .unwrap();
    let r = rig.service.check(made.id).await.unwrap();
    assert_eq!(
        (r[0].reached, r[0].key),
        (true, KeyCheck::Accepted),
        "{}",
        r[0].note_text()
    );
    assert!(!r[0].notes.contains(&Note::KeyTravelsReadable));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_host_name_or_an_unknown_ca_fails() {
    let (sink, ca) = tls_sink(&["localhost"]).await;
    let rig = Rig::with(RelayConfig::new().trust_root_for_tests(ca)).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "wrong name", &rtmps(&sink, "127.0.0.1"), "k")
        .await
        .unwrap();
    let r = rig.service.check(made.id).await.unwrap();
    assert!(
        !r[0].reached,
        "the certificate is for localhost, not for 127.0.0.1"
    );
    assert_eq!(r[0].notes, vec![Note::TlsFailed]);
    assert_eq!(
        sink.publishes.lock().unwrap().len(),
        0,
        "nothing was said past the failed TLS"
    );

    let untrusting = Rig::new().await;
    let made = untrusting.stream("s").await;
    untrusting
        .service
        .add_target(made.id, "unknown ca", &rtmps(&sink, "localhost"), "k")
        .await
        .unwrap();
    let r = untrusting.service.check(made.id).await.unwrap();
    assert!(!r[0].reached, "a CA nobody trusts is not trusted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_stream_is_sent_over_tls() {
    let (sink, ca) = tls_sink(&["localhost"]).await;
    let rig = Rig::with(RelayConfig::new().trust_root_for_tests(ca)).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &rtmps(&sink, "localhost"), "k")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    assert!(rig.service.targets(made.id).await.unwrap()[0].secure);
    raw.headers().await;
    raw.tick(0, 400).await;
    until("media arrived", LONG, || async {
        (sink.media_count() >= 4).then_some(())
    })
    .await;
}

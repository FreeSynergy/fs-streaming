//! The relay's own address, what it tells senders, and what it reports when it cannot listen.

mod common;

use std::sync::Arc;

use common::rig::Rig;
use fs_streaming_core::{Error, Note, StreamService, StreamState, Streaming, Streams};
use fs_streaming_relay::{RelayConfig, RtmpRelay};

fn own(e: &Error) -> bool {
    matches!(e, Error::InvalidInput(m) if m.contains("own"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_at_the_relays_own_address_is_refused_also_through_a_wildcard_bind() {
    for wildcard in [false, true] {
        let cfg = if wildcard {
            RelayConfig::new().bind("0.0.0.0:0".parse().unwrap())
        } else {
            RelayConfig::new()
        };
        let rig = Rig::with(cfg).await;
        let made = rig.stream("s").await;
        let port = rig.addr.port();
        let mut hosts = vec![
            "127.0.0.1",
            "localhost",
            "127.1.2.3",
            "[::ffff:127.0.0.1]",
            "0.0.0.0",
            "[::]",
        ];
        if wildcard {
            hosts.push("[::1]");
        }
        for host in hosts {
            let url = format!("rtmp://{host}:{port}/live/{}", made.id);
            let e = rig
                .service
                .add_target(made.id, "loop", &url, &made.key)
                .await
                .unwrap_err();
            assert!(own(&e), "{host} wildcard={wildcard}");
        }
        let fine = rig
            .service
            .add_target(
                made.id,
                "ok",
                &format!("rtmp://127.0.0.1:{}/live/x", port.wrapping_add(1).max(2)),
                "k",
            )
            .await
            .unwrap();
        let e = rig
            .service
            .edit_target(fine.id, "ok", &format!("rtmp://127.0.0.1:{port}/live/x"))
            .await
            .unwrap_err();
        assert!(own(&e), "an edit to the own address too");
        assert_eq!(rig.service.targets(made.id).await.unwrap().len(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wildcard_bind_never_advertises_a_wildcard_address() {
    let wild = RelayConfig::new().bind("0.0.0.0:0".parse().unwrap());
    let rig = Rig::with(wild.clone()).await;
    let url = rig.stream("s").await.url;
    assert!(
        !url.contains("0.0.0.0") && url.starts_with("rtmp://localhost:"),
        "{url}"
    );
    let rig = Rig::with(wild.advertise("stream.example.org")).await;
    let url = rig.stream("s").await.url;
    assert!(
        url.starts_with("rtmp://stream.example.org:") && url.contains("/live/"),
        "{url}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_that_cannot_listen_puts_the_streams_in_error_with_a_note() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    let store = rig.store.clone();
    let second = StreamService::new(store);
    let busy: std::net::SocketAddr = rig.addr;
    let failed = RtmpRelay::start(second.clone(), RelayConfig::new().bind(busy)).await;
    assert!(failed.is_err(), "the port is taken");
    let s = second.get(made.id).await.unwrap().unwrap();
    assert_eq!(
        (s.state, s.note),
        (StreamState::Error, Some(Note::CannotListen))
    );
    let other = rig.service.get(made.id).await.unwrap().unwrap();
    assert_eq!(
        other.state,
        StreamState::Idle,
        "only the failed service reports it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recording_is_unsupported_without_a_sink_and_supported_with_one() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    assert!(!rig.service.capabilities().is_supported("recording.start"));
    let e = rig.service.set_recording(made.id, true).await.unwrap_err();
    assert!(matches!(e, Error::Unsupported(_)), "{e}");
    assert!(
        rig.service.set_recording(made.id, false).await.is_ok(),
        "switching it off is always fine"
    );
    let dir = common::rig::scratch("caps");
    let with = Rig::recording(&dir.0).await;
    assert!(with.service.capabilities().is_supported("recording.start"));
    let _ = Arc::strong_count(&with.service);
}

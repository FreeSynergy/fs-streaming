//! Limits against slow, silent and hostile peers: they end only their own connection.

mod common;

use std::time::Duration;

use common::raw::{Answer, Raw};
use common::rig::{Made, Rig};
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{StreamState, Streams, TargetState};
use fs_streaming_relay::{Limits, RelayConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn closed_within(io: &mut TcpStream, limit: Duration) -> bool {
    let mut b = [0u8; 4096];
    let end = std::time::Instant::now() + limit;
    while std::time::Instant::now() < end {
        let read = tokio::time::timeout(Duration::from_millis(200), io.read(&mut b)).await;
        if matches!(read, Ok(Ok(0) | Err(_))) {
            return true;
        }
    }
    false
}

async fn live(rig: &Rig, made: &Made) -> Raw {
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    raw
}

async fn state(rig: &Rig, made: &Made) -> StreamState {
    rig.service.get(made.id).await.unwrap().unwrap().state
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_chunk_size_is_refused() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    let mut raw = live(&rig, &made).await;
    raw.set_chunk_size(100 * 1024 * 1024).await;
    assert!(
        closed_within(&mut raw.io, LONG).await,
        "a chunk size above 16 MiB ends the connection"
    );
    until("interrupted", LONG, || async {
        (state(&rig, &made).await != StreamState::Live).then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_message_is_refused_and_not_forwarded() {
    let limits = Limits {
        max_message: 64 * 1024,
        ..Limits::default()
    };
    let rig = Rig::with(RelayConfig::new().limits(limits)).await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &sink.url("live"), "k")
        .await
        .unwrap();
    let mut raw = live(&rig, &made).await;
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    raw.headers().await;
    raw.tick(0, 1000).await;
    raw.video(40, &vec![0x27; 100 * 1024]).await;
    assert!(closed_within(&mut raw.io, LONG).await);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        sink.messages().iter().all(|g| g.data.len() < 64 * 1024),
        "the big message went nowhere"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_stalls_in_the_handshake_times_out() {
    let limits = Limits {
        handshake_timeout: Duration::from_millis(300),
        ..Limits::default()
    };
    let rig = Rig::with(RelayConfig::new().limits(limits)).await;
    let mut nothing = TcpStream::connect(rig.addr).await.unwrap();
    let mut half = TcpStream::connect(rig.addr).await.unwrap();
    half.write_all(&[3]).await.unwrap();
    assert!(
        closed_within(&mut nothing, LONG).await,
        "silence in the handshake ends"
    );
    assert!(
        closed_within(&mut half, LONG).await,
        "half a handshake ends"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_that_goes_silent_is_treated_as_dropped() {
    let limits = Limits {
        idle_timeout: Duration::from_millis(400),
        ..Limits::default()
    };
    let rig = Rig::with(RelayConfig::new().limits(limits)).await;
    let made = rig.stream("s").await;
    let mut raw = live(&rig, &made).await;
    assert_eq!(state(&rig, &made).await, StreamState::Live);
    until("interrupted", LONG, || async {
        (state(&rig, &made).await == StreamState::Interrupted).then_some(())
    })
    .await;
    assert!(closed_within(&mut raw.io, LONG).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_bytes_end_only_that_connection() {
    let rig = Rig::new().await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &sink.url("live"), "k")
        .await
        .unwrap();
    let mut good = live(&rig, &made).await;
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    good.headers().await;
    let mut before_handshake = TcpStream::connect(rig.addr).await.unwrap();
    before_handshake.write_all(&[0xff; 5000]).await.unwrap();
    assert!(closed_within(&mut before_handshake, LONG).await);
    let mut after_handshake = Raw::connect(rig.addr).await;
    after_handshake
        .io
        .write_all(&[
            0x03, 0xde, 0xad, 0xbe, 0xef, 0x99, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0a, 0xff, 0xff,
        ])
        .await
        .unwrap();
    after_handshake.io.write_all(&[0x7f; 3000]).await.unwrap();
    assert!(closed_within(&mut after_handshake.io, LONG).await);
    for n in 0..10 {
        good.tick(n, 400).await;
    }
    until("the good sender's media arrived", LONG, || async {
        (sink.media_count() >= 20).then_some(())
    })
    .await;
    assert_eq!(state(&rig, &made).await, StreamState::Live);
}

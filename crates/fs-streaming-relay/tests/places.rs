//! The limited places belong to connections that have not published; non-readers are cut off.

mod common;

use std::time::Duration;

use common::raw::{Answer, Raw};
use common::raw_ext::command;
use common::rig::{Made, Rig};
use common::wait::{LONG, until};
use fs_streaming_core::{StreamState, Streams};
use fs_streaming_relay::{Limits, RelayConfig};
use rml_rtmp::rml_amf0::Amf0Value;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

async fn live(rig: &Rig, made: &Made) -> Raw {
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    raw
}

async fn closed(io: &mut TcpStream) -> bool {
    let mut b = [0u8; 64];
    matches!(
        tokio::time::timeout(Duration::from_millis(300), io.read(&mut b)).await,
        Ok(Ok(0) | Err(_))
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publishing_sender_gives_its_place_back_and_a_flood_does_not_touch_it() {
    let rig = Rig::with(RelayConfig::new().limits(Limits {
        max_connections: 2,
        ..Limits::default()
    }))
    .await;
    let (a, b, c) = (
        rig.stream("a").await,
        rig.stream("b").await,
        rig.stream("c").await,
    );
    let (mut ra, _rb) = (live(&rig, &a).await, live(&rig, &b).await);
    // Both places are free again: two idle sockets are taken, a third is turned away.
    let (mut one, mut two) = (
        TcpStream::connect(rig.addr).await.unwrap(),
        TcpStream::connect(rig.addr).await.unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut three = TcpStream::connect(rig.addr).await.unwrap();
    assert!(
        closed(&mut three).await,
        "only two places for connections that have not published"
    );
    assert!(
        !closed(&mut one).await && !closed(&mut two).await,
        "the flood holds its places"
    );
    ra.headers().await;
    for id in [&a, &b] {
        assert_eq!(
            rig.service.get(id.id).await.unwrap().unwrap().state,
            StreamState::Live,
            "running streams are not touched"
        );
    }
    drop((one, two));
    until("a third sender gets in", LONG, || async {
        let mut r = Raw::connect(rig.addr).await;
        (r.publish(&format!("live/{}", c.id), &c.key).await == Answer::Accepted).then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_stops_reading_is_cut_off_by_the_publish_deadline() {
    let limits = Limits {
        publish_deadline: Duration::from_millis(800),
        ..Limits::default()
    };
    let rig = Rig::with(RelayConfig::new().limits(limits)).await;
    let mut evil = Raw::connect_with(rig.addr, Some(2048)).await;
    let started = std::time::Instant::now();
    // Thousands of commands, each answered; the answers pile up unread.
    let burst: Vec<_> = (0..2000)
        .map(|_| command("createStream", vec![Amf0Value::Null], 0))
        .collect();
    let sender = tokio::spawn(async move {
        for _ in 0..40 {
            evil.batch(&burst).await;
        }
        evil
    });
    // The relay has to close the connection itself, well before the 60 s of the write timeout.
    until("the relay hung up", Duration::from_secs(20), || async {
        sender.is_finished().then_some(())
    })
    .await;
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "not held for the 60 s of the write timeout"
    );
}

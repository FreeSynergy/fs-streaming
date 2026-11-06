//! Hostile peers: they end only their own connection, and never leave a stream stuck.

mod common;

use std::time::Duration;

use common::raw::{Answer, Raw};
use common::rig::{Made, Rig};
use common::wait::{LONG, until};
use fs_streaming_core::{StreamState, Streams};
use fs_streaming_relay::{Limits, RelayConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn closed_within(io: &mut TcpStream, limit: Duration) -> bool {
    let (mut b, end) = ([0u8; 4096], std::time::Instant::now() + limit);
    while std::time::Instant::now() < end {
        let r = tokio::time::timeout(Duration::from_millis(100), io.read(&mut b)).await;
        if matches!(r, Ok(Ok(0) | Err(_))) {
            return true;
        }
    }
    false
}

async fn state(rig: &Rig, made: &Made) -> StreamState {
    rig.service.get(made.id).await.unwrap().unwrap().state
}

async fn publishing(rig: &Rig, made: &Made) -> Raw {
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    raw
}

fn nested(levels: usize) -> Vec<u8> {
    (0..levels).flat_map(|_| [0x0a, 0, 0, 0, 1]).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_huge_nest_of_amf_arrays_right_after_the_handshake_ends_only_that_connection() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    let mut good = publishing(&rig, &made).await;
    for size in [20_000, 1_600] {
        let mut evil = Raw::connect(rig.addr).await;
        evil.send(20, 0, &nested(size)).await;
        assert!(closed_within(&mut evil.io, LONG).await, "{size} levels");
    }
    good.headers().await;
    assert_eq!(
        state(&rig, &made).await,
        StreamState::Live,
        "the process and the stream are fine"
    );
    assert!(Raw::connect(rig.addr).await.publish("live/x", "y").await != Answer::Accepted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_set_data_frame_and_interleaved_chunks_cannot_leave_the_stream_stuck() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    let mut raw = publishing(&rig, &made).await;
    let mut lone = vec![0x02, 0x00, 0x0d];
    lone.extend_from_slice(b"@setDataFrame");
    raw.send(18, 0, &lone).await;
    raw.headers().await;
    raw.tick(0, 300).await;
    assert_eq!(
        state(&rig, &made).await,
        StreamState::Live,
        "a lone @setDataFrame is nothing to worry about"
    );
    // Chunk stream 4 starts a 300 byte message, chunk stream 5 starts another, then 4 carries on.
    let mut bytes = vec![0x04, 0, 0, 0, 0, 1, 0x2c, 9, 1, 0, 0, 0];
    bytes.extend_from_slice(&[0x17; 128]);
    bytes.extend_from_slice(&[0x05, 0, 0, 0, 0, 0, 0x0a, 8, 1, 0, 0, 0]);
    bytes.extend_from_slice(&[0xAF; 10]);
    bytes.push(0xc4);
    bytes.extend_from_slice(&[0x27; 128]);
    raw.io.write_all(&bytes).await.unwrap();
    drop(raw);
    until("the stream is released", LONG, || async {
        (state(&rig, &made).await != StreamState::Live).then_some(())
    })
    .await;
    rig.service.stop(made.id).await.unwrap();
    rig.service.delete(made.id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn there_is_a_limit_on_connections_and_a_slot_comes_free_again() {
    let rig = Rig::with(RelayConfig::new().limits(Limits {
        max_connections: 4,
        ..Limits::default()
    }))
    .await;
    let made = rig.stream("s").await;
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(TcpStream::connect(rig.addr).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut fifth = TcpStream::connect(rig.addr).await.unwrap();
    assert!(
        closed_within(&mut fifth, LONG).await,
        "beyond the limit a connection is closed at once"
    );
    drop(held.pop());
    until("a slot is free", LONG, || async {
        let mut c = TcpStream::connect(rig.addr).await.ok()?;
        c.write_all(&[3]).await.ok()?;
        let mut b = [0u8; 1];
        matches!(tokio::time::timeout(Duration::from_millis(300), c.read(&mut b)).await, Ok(Ok(n)) if n > 0).then_some(())
    })
    .await;
    drop(held);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = publishing(&rig, &made).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_that_trickles_bytes_is_cut_off_before_and_after_publish() {
    let limits = Limits {
        publish_deadline: Duration::from_millis(700),
        idle_timeout: Duration::from_millis(700),
        ..Limits::default()
    };
    let rig = Rig::with(RelayConfig::new().limits(limits)).await;
    let made = rig.stream("s").await;
    let mut slow = Raw::connect(rig.addr).await;
    let started = std::time::Instant::now();
    while !closed_within(&mut slow.io, Duration::from_millis(1)).await {
        let _ = slow.io.write_all(&[0x03]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "a byte every 100 ms kept the connection open"
        );
    }
    let mut raw = publishing(&rig, &made).await;
    let started = std::time::Instant::now();
    // The start of a message that never completes, a byte at a time.
    let _ = raw
        .io
        .write_all(&[0x04, 0, 0, 0, 0, 1, 0x2c, 9, 1, 0, 0, 0])
        .await;
    while !closed_within(&mut raw.io, Duration::from_millis(1)).await {
        let _ = raw.io.write_all(&[0x17]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "trickling kept a sender alive"
        );
    }
    until("interrupted", LONG, || async {
        (state(&rig, &made).await == StreamState::Interrupted).then_some(())
    })
    .await;
}

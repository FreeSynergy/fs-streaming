//! Which media a sender may send: the right stream id, after `publish`, every message in order.

mod common;

use common::raw::{Answer, Raw};
use common::raw_ext::{command, payload};
use common::rig::{Made, Rig};
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{StreamState, Streams, TargetState};
use rml_rtmp::rml_amf0::Amf0Value;

async fn state(rig: &Rig, made: &Made) -> StreamState {
    rig.service.get(made.id).await.unwrap().unwrap().state
}

async fn publishing(rig: &Rig, made: &Made) -> Raw {
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    raw
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_media_on_the_published_stream_after_publish_is_forwarded_and_every_message_counts() {
    let rig = Rig::new().await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &sink.url("live"), "k")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    raw.prepare(&app).await;
    let sid = raw.stream_id;
    // Media before publish, in the same write as publish.
    let early = payload(9, 5, sid, &[0x17, 1, 0, 0, 0, 0xEE]);
    let publish = raw.publish_message(&name);
    raw.batch(&[early, publish]).await;
    assert!(raw.answer().await);
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    raw.headers().await;
    raw.send(9, 40, &[0x17, 1, 0, 0, 0, 0x11]).await;
    raw.batch(&[
        payload(9, 80, 77, &[0x17, 1, 0, 0, 0, 0xBB]),
        payload(9, 120, sid, &[0x27, 1, 0, 0, 0, 0x22]),
    ])
    .await;
    // A frame that shares a write with deleteStream is not lost.
    let last = payload(9, 160, sid, &[0x27, 1, 0, 0, 0, 0x33]);
    let delete = command("deleteStream", vec![Amf0Value::Number(f64::from(sid))], 0);
    raw.batch(&[last, delete]).await;
    until("the last frame", LONG, || async {
        sink.messages().iter().any(|g| g.ts == 160).then_some(())
    })
    .await;
    let got = sink.messages();
    let tail = |g: &common::sink::Got| *g.data.last().unwrap();
    assert!(
        !got.iter().any(|g| matches!(tail(g), 0xEE | 0xBB)),
        "media before publish and on stream 77 stayed out"
    );
    assert!(
        got.iter().any(|g| tail(g) == 0x22),
        "the frame after it on the right stream is in"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_amf_date_does_not_disconnect_a_sender() {
    let rig = Rig::new().await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("s").await;
    rig.service
        .add_target(made.id, "t", &sink.url("live"), "k")
        .await
        .unwrap();
    let mut raw = publishing(&rig, &made).await;
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    let mut meta = vec![0x02, 0x00, 0x0a];
    meta.extend_from_slice(b"onMetaData");
    meta.extend_from_slice(&[0x0b, 0x40, 0x8f, 0x40, 0, 0, 0, 0, 0, 0, 0]);
    raw.headers().await;
    raw.send(18, 0, &meta).await;
    raw.send(
        20,
        0,
        &[
            0x02, 0, 3, b'f', b'o', b'o', 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0x0b, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ],
    )
    .await;
    raw.tick(0, 300).await;
    until("media after the date arrived", LONG, || async {
        (sink.media_count() >= 4).then_some(())
    })
    .await;
    assert_eq!(state(&rig, &made).await, StreamState::Live);
    assert!(
        sink.messages()
            .iter()
            .any(|g| g.type_id == 18 && g.data.as_ref() == meta.as_slice()),
        "the metadata went through untouched"
    );
}

//! End to end: a real `ffmpeg` publishes to the relay, two further relay instances are the targets,
//! and `ffprobe` reads what arrives. A raw client proves the bytes are untouched.

mod common;

use std::time::Duration;

use common::ff;
use common::raw::{Answer, Raw, metadata};
use common::rig::{Rig, scratch};
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{StreamState, Streams, TargetState};

fn recordings(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .map(|d| d.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ffmpeg_through_the_relay_to_two_more_relays_and_a_recording() {
    if ff::skip() {
        return;
    }
    let dir = scratch("e2e");
    let video = dir.0.join("src.flv");
    ff::make_video(&video, 4);
    let (own, t1, t2) = (dir.0.join("own"), dir.0.join("t1"), dir.0.join("t2"));
    let main = Rig::recording(&own).await;
    let (r1, r2) = (Rig::recording(&t1).await, Rig::recording(&t2).await);
    let (s1, s2) = (r1.stream("target one").await, r2.stream("target two").await);
    r1.service.set_recording(s1.id, true).await.unwrap();
    r2.service.set_recording(s2.id, true).await.unwrap();
    let made = main.stream("main").await;
    main.service.set_recording(made.id, true).await.unwrap();
    main.add_target_to(&made, &s1, "one").await;
    main.add_target_to(&made, &s2, "two").await;

    let mut child = ff::spawn_publish(&video, &made.full_url());
    until("the stream is live", LONG, || async {
        (main.service.get(made.id).await.unwrap().unwrap().state == StreamState::Live).then_some(())
    })
    .await;
    let st = child.wait().await.unwrap();
    assert!(st.success(), "ffmpeg could not publish");
    until("the relay saw the end", LONG, || async {
        (main.service.status(made.id).await.unwrap().bytes_in > 100_000).then_some(())
    })
    .await;
    let status = main.service.status(made.id).await.unwrap();
    assert!(status.has_video && status.has_audio);
    for t in &status.targets {
        assert!(t.bytes_out > 100_000, "every target got the stream");
        assert_eq!(t.dropped, 0);
    }
    main.service.stop(made.id).await.unwrap();
    for r in [&r1, &r2] {
        let id = if std::ptr::eq(r, &raw const r1) {
            s1.id
        } else {
            s2.id
        };
        r.service.stop(id).await.unwrap();
    }
    for (name, folder) in [("own", &own), ("target one", &t1), ("target two", &t2)] {
        let files = until("the recording is closed", LONG, || async {
            Some(recordings(folder)).filter(|f| !f.is_empty())
        })
        .await;
        assert_eq!(files.len(), 1, "{name}: one file");
        let p = ff::probe(&files[0]);
        assert_eq!(p.video.as_deref(), Some("h264"), "{name}");
        assert_eq!(p.audio.as_deref(), Some("aac"), "{name}");
        assert!(
            (3.0..5.5).contains(&p.duration),
            "{name}: duration {}",
            p.duration
        );
        assert_eq!(ff::decode_errors(&files[0]), "", "{name}: decodes cleanly");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_and_timestamps_are_byte_identical() {
    let rig = Rig::new().await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("raw").await;
    rig.service
        .add_target(made.id, "sink", &sink.url("live"), "outkey")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    until("the target is sending", LONG, || async {
        let t = rig.service.targets(made.id).await.unwrap();
        (t[0].state == TargetState::Sending).then_some(())
    })
    .await;
    let meta = metadata();
    let vhdr = [0x17u8, 0, 0, 0, 0, 0x01, 0x64, 0x00, 0x1f];
    let ahdr = [0xAFu8, 0x00, 0x12, 0x10];
    raw.send(18, 0, &meta).await;
    raw.video(0, &vhdr).await;
    raw.audio(0, &ahdr).await;
    let mut sent: Vec<(u8, u32, Vec<u8>)> =
        vec![(18, 0, meta), (9, 0, vhdr.to_vec()), (8, 0, ahdr.to_vec())];
    for i in 1..=30u32 {
        let ts = i * 40 + 7;
        let mut v = vec![if i % 10 == 1 { 0x17 } else { 0x27 }, 1, 0, 0, 0];
        v.extend((0..(200 + i)).map(|b| u8::try_from((b * 7 + i) & 0xFF).unwrap()));
        raw.video(ts, &v).await;
        sent.push((9, ts, v));
        let a: Vec<u8> = [0xAF, 0x01]
            .into_iter()
            .chain((0..50).map(|b| u8::try_from((b + i) & 0xFF).unwrap()))
            .collect();
        raw.audio(ts, &a).await;
        sent.push((8, ts, a));
    }
    until("all arrived", LONG, || async {
        (sink.messages().len() >= sent.len()).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let got = sink.messages();
    assert_eq!(got.len(), sent.len(), "no message lost or added");
    for (g, (t, ts, data)) in got.iter().zip(sent.iter()) {
        assert_eq!((g.type_id, g.ts), (*t, *ts), "type and timestamp kept");
        assert_eq!(g.data.as_ref(), data.as_slice(), "payload untouched");
    }
}

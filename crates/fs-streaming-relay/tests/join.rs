//! Joining mid-stream: headers first, then a keyframe. Enhanced RTMP passes through untouched.

mod common;

use common::ff;
use common::raw::{Answer, Raw, metadata};
use common::rig::{Rig, scratch};
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_streaming_core::{StreamState, Streams, TargetState};

async fn wait_live(rig: &Rig, id: fs_streaming_core::ObjectId) {
    until("live", LONG, || async {
        (rig.service.get(id).await.unwrap().unwrap().state == StreamState::Live).then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_added_or_enabled_while_live_starts_clean_at_a_keyframe() {
    if ff::skip() {
        return;
    }
    let dir = scratch("join");
    let video = dir.0.join("src.flv");
    ff::make_video(&video, 8);
    let main = Rig::new().await;
    let (late, later) = (
        Rig::recording(&dir.0.join("late")).await,
        Rig::recording(&dir.0.join("later")).await,
    );
    let (a, b) = (late.stream("late").await, later.stream("later").await);
    late.service.set_recording(a.id, true).await.unwrap();
    later.service.set_recording(b.id, true).await.unwrap();
    let made = main.stream("main").await;
    let mut child = ff::spawn_publish(&video, &made.full_url());
    wait_live(&main, made.id).await;
    until("some media passed", LONG, || async {
        (main.service.status(made.id).await.unwrap().bytes_in > 60_000).then_some(())
    })
    .await;
    let t_a = main.add_target_to(&made, &a, "added while live").await;
    let t_b = main
        .add_target_to(&made, &b, "switched on while live")
        .await;
    main.service
        .set_target_enabled(t_b.id, false)
        .await
        .unwrap();
    assert_eq!(
        main.service.targets(made.id).await.unwrap()[1].state,
        TargetState::Off
    );
    until("the added target sends", LONG, || async {
        (main.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending)
            .then_some(())
    })
    .await;
    main.service.set_target_enabled(t_b.id, true).await.unwrap();
    assert!(child.wait().await.unwrap().success());
    until("the second target sent too", LONG, || async {
        let s = main.service.status(made.id).await.unwrap();
        s.targets.iter().all(|t| t.bytes_out > 10_000).then_some(())
    })
    .await;
    assert_ne!(t_a.id, t_b.id);
    main.service.stop(made.id).await.unwrap();
    late.service.stop(a.id).await.unwrap();
    later.service.stop(b.id).await.unwrap();
    for name in ["late", "later"] {
        let folder = dir.0.join(name);
        let files = until("closed recording", LONG, || async {
            let v: Vec<_> = std::fs::read_dir(&folder)
                .map(|d| d.flatten().map(|e| e.path()).collect())
                .unwrap_or_default();
            Some(v).filter(|v: &Vec<std::path::PathBuf>| !v.is_empty())
        })
        .await;
        let p = ff::probe(&files[0]);
        assert_eq!(
            (p.video.as_deref(), p.audio.as_deref()),
            (Some("h264"), Some("aac")),
            "{name}"
        );
        assert!(
            p.start > 0.3,
            "{name} joined in the middle, not at the start: {}",
            p.start
        );
        assert_eq!(
            ff::decode_errors(&files[0]),
            "",
            "{name}: decodes without errors from the first keyframe"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enhanced_rtmp_av01_passes_through_and_its_keyframe_bit_is_recognised() {
    let rig = Rig::new().await;
    let sink = Sink::start(Mode::Accept).await;
    let made = rig.stream("enhanced").await;
    rig.service
        .add_target(made.id, "sink", &sink.url("live"), "k")
        .await
        .unwrap();
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    until("sending", LONG, || async {
        (rig.service.targets(made.id).await.unwrap()[0].state == TargetState::Sending).then_some(())
    })
    .await;
    let av01 = |first: u8, tail: &[u8]| [&[first][..], b"av01", tail].concat();
    let header = av01(0x90, &[0x81, 0x00, 0x0c, 0x00]);
    let inter = av01(0xA1, &[1, 2, 3]);
    let key = av01(0x91, &[9, 8, 7, 6]);
    raw.send(18, 0, &metadata()).await;
    raw.video(0, &header).await;
    raw.audio(0, &[0xAF, 0, 0x12, 0x10]).await;
    for i in 1..=3 {
        raw.video(i * 40, &inter).await;
    }
    raw.video(160, &key).await;
    raw.video(200, &inter).await;
    until("the stream arrived", LONG, || async {
        (sink.messages().len() >= 5).then_some(())
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let got = sink.messages();
    let video: Vec<_> = got.iter().filter(|g| g.type_id == 9).collect();
    assert_eq!(
        video.len(),
        3,
        "header, keyframe, one inter frame: the inter frames before the keyframe were left out"
    );
    assert_eq!(
        (
            video[0].data.as_ref(),
            video[1].data.as_ref(),
            video[2].data.as_ref()
        ),
        (&header[..], &key[..], &inter[..])
    );
    assert_eq!((video[1].ts, video[2].ts), (160, 200));
    assert!(
        got[0].type_id == 18 && got[1].type_id == 9 && got[2].type_id == 8,
        "headers first"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn av1_from_ffmpeg_is_recorded_as_enhanced_rtmp() {
    if ff::skip() {
        return;
    }
    let dir = scratch("av1");
    let video = dir.0.join("src.flv");
    ff::make_video_with(&video, 3, &["-c:v", "libsvtav1", "-g", "25"]);
    let rig = Rig::recording(&dir.0.join("rec")).await;
    let made = rig.stream("av1").await;
    rig.service.set_recording(made.id, true).await.unwrap();
    let mut child = ff::spawn_publish(&video, &made.full_url());
    assert!(child.wait().await.unwrap().success());
    until("bytes", LONG, || async {
        (rig.service.status(made.id).await.unwrap().bytes_in > 5_000).then_some(())
    })
    .await;
    rig.service.stop(made.id).await.unwrap();
    let folder = dir.0.join("rec");
    let files = until("recording", LONG, || async {
        let v: Vec<_> = std::fs::read_dir(&folder)
            .map(|d| d.flatten().map(|e| e.path()).collect())
            .unwrap_or_default();
        Some(v).filter(|v: &Vec<std::path::PathBuf>| !v.is_empty())
    })
    .await;
    let p = ff::probe(&files[0]);
    assert_eq!(p.video.as_deref(), Some("av1"));
    assert_eq!(ff::decode_errors(&files[0]), "");
}

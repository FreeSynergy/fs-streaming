//! Secrets: a marker key is put through everything, and every output is searched for it.

mod common;

use std::sync::Arc;

use common::capture::{Capture, Feed, contains, files_under};
use common::raw::{Answer, Raw};
use common::rig::{Rig, scratch};
use common::sink::{Mode, Sink};
use common::wait::{LONG, until};
use fs_store::TursoStore;
use fs_store::traits::UnloggedObjects;
use fs_streaming_core::{Note, Repo, Secret, Streaming, Streams, TargetState};
use fs_streaming_relay::RelayConfig;

const MARKER: &str = "MARKERKEY0123456789abcdefMARKER";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marker_key_appears_only_in_the_private_stores_target_row() {
    let log = Capture::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(log.clone())
            .finish(),
    )
    .unwrap();
    let dir = scratch("secrets");
    let store = Arc::new(TursoStore::open(dir.0.join("private.db")).await.unwrap());
    let rig = Rig::with_store(store.clone(), RelayConfig::new()).await;
    let (refuse, accept) = (
        Sink::start(Mode::Refuse).await,
        Sink::start(Mode::Accept).await,
    );
    let feed = Arc::new(Feed::default());
    let _sub = rig.service.subscribe(feed.clone()).await.unwrap();
    let mut seen: Vec<String> = vec![format!("{:?}", Secret::new(MARKER))];

    let created = rig.service.create("secret stream").await.unwrap();
    let publisher = created.publisher_key.expose().to_string();
    seen.push(format!("{created:?}"));
    let id = created.stream.id;
    let refused = rig
        .service
        .add_target(id, "refused", &refuse.url("live"), MARKER)
        .await
        .unwrap();
    let kept = rig
        .service
        .add_target(id, "kept", &accept.url("live"), MARKER)
        .await
        .unwrap();
    assert!(refused.has_key && kept.has_key);

    for bad in [
        format!("rtmp://user:{MARKER}@example.org/app"),
        format!("rtmp://example.org/app?key={MARKER}"),
        format!("rtmp://example.org/app#{MARKER}"),
        format!("http://example.org/{MARKER}"),
    ] {
        let e = rig
            .service
            .add_target(id, "bad", &bad, MARKER)
            .await
            .unwrap_err();
        seen.push(format!("{e} / {e:?}"));
        assert!(matches!(e, fs_streaming_core::Error::InvalidUrl(_)), "{e}");
    }
    let e = rig
        .service
        .set_target_key(kept.id, &format!("a/{MARKER}"))
        .await
        .unwrap_err();
    seen.push(format!("{e} / {e:?}"));
    assert_eq!(
        rig.service.targets(id).await.unwrap().len(),
        2,
        "refused addresses were not added"
    );

    let checked = rig.service.check(id).await.unwrap();
    seen.push(format!("{checked:?}"));
    assert_eq!(checked[0].key, fs_streaming_core::KeyCheck::Rejected);
    let mut raw = Raw::connect(rig.addr).await;
    let (app, name) = (format!("live/{id}"), publisher.clone());
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    until("one sends, one is refused", LONG, || async {
        let t = rig.service.targets(id).await.unwrap();
        (t[0].state == TargetState::Refused && t[1].state == TargetState::Sending).then_some(())
    })
    .await;
    raw.headers().await;
    raw.tick(0, 300).await;
    seen.push(format!("{:?}", rig.service.targets(id).await.unwrap()));
    seen.push(format!("{:?}", rig.service.status(id).await.unwrap()));
    seen.push(format!("{:?}", rig.service.get(id).await.unwrap()));
    let note = rig.service.targets(id).await.unwrap()[0].note.unwrap();
    assert_eq!(note, Note::PublishRefused, "our words, not the far end's");
    let repo = Repo::new(store.clone());
    seen.push(format!("{:?}", repo.all().await.unwrap()));

    // The only thing stored about the publisher key is its hash.
    let obj = store.read_unlogged(id).await.unwrap().unwrap();
    let text = obj.content.to_string();
    assert!(!text.contains(&publisher));
    assert!(text.contains(&fs_streaming_core::keys::hash(&publisher)));
    // The marker is in the target rows, and nowhere else in what is stored.
    let rows = repo.all().await.unwrap();
    assert!(
        rows[0]
            .targets
            .iter()
            .all(|t| t.key.as_deref() == Some(MARKER))
    );
    let without_keys = text.replace(&format!("\"key\":\"{MARKER}\""), "");
    assert!(!without_keys.contains(MARKER));

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    rig.service.stop(id).await.unwrap();
    drop(raw);
    for s in feed.0.lock().unwrap().iter() {
        seen.push(s.clone());
    }
    let logged = String::from_utf8_lossy(&log.0.lock().unwrap()).to_string();
    assert!(!logged.is_empty(), "the relay did log something");
    seen.push(logged);
    for (i, text) in seen.iter().enumerate() {
        assert!(
            !text.contains(MARKER),
            "the marker key leaked into output {i}: {text}"
        );
        assert!(
            !text.contains(&publisher),
            "the publisher key leaked into output {i}"
        );
    }
    assert!(feed.0.lock().unwrap().len() > 5, "the feed carried events");

    let mut raw_files = Vec::new();
    files_under(&dir.0, &mut raw_files);
    assert!(
        raw_files.iter().all(|f| !contains(f, &publisher)),
        "the publisher key is on no disk"
    );
    assert!(
        raw_files.iter().any(|f| contains(f, MARKER)),
        "the target key is in the private store"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_host_or_plain_rtmp_clears_the_key_and_a_new_path_keeps_it() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    let secure = rig
        .service
        .add_target(made.id, "t", "rtmps://a.example.org/live", MARKER)
        .await
        .unwrap();
    let has_key = |rig: &Rig, id| {
        let s = rig.service.clone();
        async move {
            s.targets(made.id)
                .await
                .unwrap()
                .into_iter()
                .find(|t| t.id == id)
                .unwrap()
                .has_key
        }
    };
    rig.service
        .edit_target(secure.id, "t", "rtmps://a.example.org/other/path")
        .await
        .unwrap();
    assert!(has_key(&rig, secure.id).await, "same host: the key stays");
    rig.service
        .edit_target(secure.id, "t", "rtmp://a.example.org/live")
        .await
        .unwrap();
    assert!(
        !has_key(&rig, secure.id).await,
        "rtmps to rtmp clears the key"
    );
    rig.service.set_target_key(secure.id, MARKER).await.unwrap();
    rig.service
        .edit_target(secure.id, "t", "rtmp://b.example.org/live")
        .await
        .unwrap();
    assert!(
        !has_key(&rig, secure.id).await,
        "another host clears the key"
    );
    rig.service.set_target_key(secure.id, MARKER).await.unwrap();
    rig.service.set_target_key(secure.id, "").await.unwrap();
    assert!(!has_key(&rig, secure.id).await, "an empty key removes it");
}

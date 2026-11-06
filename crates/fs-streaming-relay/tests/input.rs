//! Unknown ids and unacceptable values are refused, in our words.

mod common;

use common::rig::Rig;
use fs_streaming_core::{Error, ObjectId, StreamState, Streams};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_ids_and_bad_values_are_refused_in_our_words() {
    let rig = Rig::new().await;
    let nobody = ObjectId::new();
    assert!(matches!(
        rig.service.rename(nobody, "x").await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        rig.service.rotate_key(nobody).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        rig.service.status(nobody).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        rig.service.check(nobody).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        rig.service.add_target(nobody, "t", "rtmp://h/a", "").await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        rig.service.remove_target(nobody).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        rig.service.create("  ").await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        rig.service.create("bad\nname").await,
        Err(Error::InvalidInput(_))
    ));
    let made = rig.stream("ok").await;
    for url in [
        "http://h/a",
        "rtmp://h",
        "rtmp://h/",
        "rtmp://:1/a",
        "rtmp://h:0/a",
        "rtmp://h:99999/a",
        "rtmp://u@h/a",
        "rtmp://h/a?x=1",
        "rtmp://h/a#f",
        "rtmp://h /a",
    ] {
        assert!(
            matches!(
                rig.service.add_target(made.id, "t", url, "").await,
                Err(Error::InvalidUrl(_))
            ),
            "{url}"
        );
    }
    assert!(
        matches!(
            rig.service
                .add_target(made.id, "t", "rtmp://h/a", "bad key")
                .await
                .map(|_| ()),
            Ok(())
        ),
        "spaces in a key are the user's business"
    );
    assert!(matches!(
        rig.service
            .add_target(made.id, "t", "rtmp://h/a", "bad\nkey")
            .await,
        Err(Error::InvalidInput(_))
    ));
    let stopped = rig.service.stop(made.id).await.unwrap();
    assert_eq!(
        stopped.state,
        StreamState::Idle,
        "stop on a stream nobody sends to changes nothing"
    );
    let renamed = rig.service.rename(made.id, "  new  ").await.unwrap();
    assert_eq!(renamed.name, "new");
}

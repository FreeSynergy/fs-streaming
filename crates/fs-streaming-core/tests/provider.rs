//! The provider rule (ADR 0045): every method exists, what is missing says `Unsupported`.

use async_trait::async_trait;
use fs_interfaces::Capabilities;
use fs_streaming_core::{
    Cursor, Error, ObjectId, Page, Result, Status, Stream, Streaming, Streams, Target,
};

/// A provider that can only read.
struct ReadOnly;

#[async_trait]
impl Streams for ReadOnly {
    async fn get(&self, _: ObjectId) -> Result<Option<Stream>> {
        Ok(None)
    }
    async fn list(&self, _: Option<&Cursor>) -> Result<Page<Stream>> {
        Ok(Page {
            items: vec![],
            next: None,
        })
    }
    async fn status(&self, _: ObjectId) -> Result<Status> {
        Err(Error::NotFound("stream"))
    }
    async fn targets(&self, _: ObjectId) -> Result<Vec<Target>> {
        Ok(vec![])
    }
}

#[async_trait]
impl Streaming for ReadOnly {
    fn capabilities(&self) -> Capabilities {
        let mut c = Capabilities::new("read-only");
        c.supported("stream.read")
            .unsupported("stream.create", "this source cannot be written");
        c
    }
}

fn unsupported_id<T: std::fmt::Debug>(r: Result<T>) -> String {
    match r {
        Err(Error::Unsupported(u)) => u.capability().to_string(),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[tokio::test]
async fn what_a_provider_does_not_override_says_unsupported_with_its_capability_id() {
    let p = ReadOnly;
    let id = ObjectId::new();
    assert_eq!(unsupported_id(p.create("x").await), "stream.create");
    assert_eq!(
        unsupported_id(p.rotate_key(id).await),
        "publisher.authorize"
    );
    assert_eq!(unsupported_id(p.rename(id, "x").await), "stream.create");
    assert_eq!(unsupported_id(p.delete(id).await), "stream.create");
    assert_eq!(unsupported_id(p.stop(id).await), "stream.stop");
    assert_eq!(
        unsupported_id(p.set_recording(id, true).await),
        "recording.start"
    );
    assert_eq!(
        unsupported_id(p.add_target(id, "t", "rtmp://h/a", "").await),
        "restream.add"
    );
    assert_eq!(
        unsupported_id(p.edit_target(id, "t", "rtmp://h/a").await),
        "restream.add"
    );
    assert_eq!(
        unsupported_id(p.set_target_key(id, "").await),
        "restream.add"
    );
    assert_eq!(
        unsupported_id(p.set_target_enabled(id, true).await),
        "restream.add"
    );
    assert_eq!(unsupported_id(p.remove_target(id).await), "restream.add");
    assert_eq!(unsupported_id(p.check(id).await), "stream.check");
    struct Nobody;
    #[async_trait]
    impl fs_streaming_core::StatusListener for Nobody {
        async fn stream(&self, _: Stream) {}
        async fn target(&self, _: Target) {}
        async fn recording(&self, _: fs_streaming_core::RecordingInfo) {}
        async fn removed(&self, _: ObjectId) {}
        async fn done(&self) {}
    }
    let sub = p.subscribe(std::sync::Arc::new(Nobody)).await;
    assert_eq!(unsupported_id(sub.map(|_| ())), "stream.subscribe");
    let caps = p.capabilities();
    assert!(caps.why_not("stream.create").is_some());
}

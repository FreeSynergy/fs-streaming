//! The capability report of the service: every id of the contract, the left-out ones with a reason.

use std::sync::Arc;

use fs_streaming_core::{StreamService, Streaming};

async fn caps() -> fs_interfaces::Capabilities {
    let store = Arc::new(fs_store::TursoStore::open_in_memory().await.unwrap());
    StreamService::new(store).capabilities()
}

#[tokio::test]
async fn every_capability_named_in_the_contract_is_reported() {
    let c = caps().await;
    for id in [
        "stream.create",
        "stream.read",
        "stream.stop",
        "stream.check",
        "stream.subscribe",
        "publisher.authorize",
        "restream.add",
    ] {
        assert!(c.is_supported(id), "{id}");
    }
}

#[tokio::test]
async fn what_the_contract_leaves_out_is_unsupported_with_a_reason() {
    let c = caps().await;
    for id in [
        "source.pull",
        "viewer.authorize",
        "hook.send",
        "ingest.rtmps",
        "ingest.srt",
        "ingest.rtsp",
        "ingest.whip",
        "egress.hls",
        "egress.dash",
        "egress.whep",
        "recording.segments",
        "publisher.authorize.other",
        "check.format",
        "publish.offline",
        "capture",
        "codec.decode",
        "codec.encode",
    ] {
        let u = c
            .why_not(id)
            .unwrap_or_else(|| panic!("{id} is not reported as unsupported"));
        assert!(!u.reason().is_empty(), "{id} has a reason");
        assert!(!c.is_supported(id));
    }
}

#[tokio::test]
async fn recording_is_unsupported_without_a_relay_that_can_record() {
    let c = caps().await;
    assert!(!c.is_supported("recording.start"));
    assert!(!c.why_not("recording.start").unwrap().reason().is_empty());
}

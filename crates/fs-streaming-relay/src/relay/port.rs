//! `RtmpRelay` as the core's `Relay`.
//!
//! Pattern: Adapter — the core's port calls become calls on live streams and checks.

use async_trait::async_trait;
use fs_streaming_core::{CheckResult, ObjectId, Relay, RelayStatus, TargetSpec};

use super::RtmpRelay;
use super::check::run_checks;

#[async_trait]
impl Relay for RtmpRelay {
    fn ingest_base(&self) -> String {
        format!("rtmp://{}/live", self.local_addr())
    }

    async fn stop_sender(&self, stream: ObjectId) {
        self.cancel_check(stream);
        let live = self.shared.live.lock().await.get(&stream).cloned();
        if let Some(live) = live {
            live.end(false).await;
        }
    }

    async fn apply_target(&self, stream: ObjectId, target: TargetSpec, enabled: bool) {
        let live = self.shared.live.lock().await.get(&stream).cloned();
        if let Some(live) = live {
            live.apply_target(target, enabled).await;
        }
    }

    async fn drop_target(&self, target: ObjectId) {
        let all: Vec<_> = self.shared.live.lock().await.values().cloned().collect();
        for live in all {
            live.drop_target(target).await;
        }
    }

    async fn set_recording(&self, stream: ObjectId, on: bool) {
        let live = self.shared.live.lock().await.get(&stream).cloned();
        if let Some(live) = live {
            live.set_recording(on).await;
        }
    }

    async fn status(&self, stream: ObjectId) -> Option<RelayStatus> {
        let live = self.shared.live.lock().await.get(&stream).cloned()?;
        Some(live.status().await)
    }

    async fn check(&self, stream: ObjectId, targets: Vec<TargetSpec>) -> Vec<CheckResult> {
        run_checks(&self.shared, stream, targets).await
    }

    async fn forget(&self, stream: ObjectId) {
        self.stop_sender(stream).await;
    }
}

impl RtmpRelay {
    pub(super) fn cancel_check(&self, stream: ObjectId) {
        if let Some(t) = self
            .shared
            .checks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&stream)
        {
            t.cancel();
        }
    }
}

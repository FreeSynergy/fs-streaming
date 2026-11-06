//! `RtmpRelay` as the core's `Relay`.
//!
//! Pattern: Adapter — the core's port calls become calls on live streams and checks.

use std::net::IpAddr;

use async_trait::async_trait;
use fs_streaming_core::{CancelToken, CheckResult, ObjectId, Relay, RelayStatus, TargetSpec};

use super::RtmpRelay;
use super::check::run_checks;

#[async_trait]
impl Relay for RtmpRelay {
    fn ingest_base(&self) -> String {
        let addr = self.local_addr();
        let host = match &self.shared.cfg.advertise {
            Some(h) => h.clone(),
            None if addr.ip().is_unspecified() => "localhost".to_string(),
            None => addr.ip().to_string(),
        };
        let host = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host
        };
        format!("rtmp://{host}:{}/live", addr.port())
    }

    fn can_record(&self) -> bool {
        self.shared.cfg.sink.is_some()
    }

    async fn is_own_address(&self, host: &str, port: u16) -> bool {
        let bind = self.local_addr();
        if port != bind.port() {
            return false;
        }
        let ips: Vec<IpAddr> = match host.parse::<IpAddr>() {
            Ok(ip) => vec![ip],
            Err(_) => tokio::net::lookup_host((host, port))
                .await
                .map(|a| a.map(|s| s.ip()).collect())
                .unwrap_or_default(),
        };
        ips.iter().any(|ip| match bind.ip() {
            b if b.is_unspecified() => {
                ip.is_unspecified() || std::net::UdpSocket::bind((*ip, 0)).is_ok()
            }
            b if b.is_loopback() => ip.is_loopback(),
            b => *ip == b,
        })
    }

    async fn stop_sender(&self, stream: ObjectId) {
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

    async fn check(&self, targets: Vec<TargetSpec>, cancel: CancelToken) -> Vec<CheckResult> {
        run_checks(&self.shared, targets, cancel).await
    }

    async fn forget(&self, stream: ObjectId) {
        self.stop_sender(stream).await;
    }
}

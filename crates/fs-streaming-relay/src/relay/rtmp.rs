//! `RtmpRelay`: listens for senders and implements the `Relay` port.
//!
//! Pattern: Facade — the host creates one with a [`Gate`] (the core's service) and a [`RelayConfig`],
//! attaches it to the service, and calls `shutdown` when done. It owns the accept task; every other
//! task belongs to a live stream or a check and ends with it.

use std::net::SocketAddr;
use std::sync::Arc;

use fs_streaming_core::Gate;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use super::Shared;
use crate::config::RelayConfig;
use crate::ingest;
use crate::task::Task;

/// The RTMP relay.
pub struct RtmpRelay {
    pub(super) shared: Arc<Shared>,
    local: SocketAddr,
    accept: Mutex<Option<Task>>,
}

impl RtmpRelay {
    /// Bind and start accepting senders.
    pub async fn start(gate: Arc<dyn Gate>, cfg: RelayConfig) -> std::io::Result<Arc<Self>> {
        let listener = TcpListener::bind(cfg.bind).await?;
        let local = listener.local_addr()?;
        let shared = Shared::new(gate, cfg);
        let accept = Task::spawn(ingest::accept_loop(listener, shared.clone()));
        Ok(Arc::new(Self {
            shared,
            local,
            accept: Mutex::new(Some(accept)),
        }))
    }

    /// The address senders connect to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    /// Stop accepting, end every stream and every check. Nothing is left running.
    pub async fn shutdown(&self) {
        if let Some(t) = self.accept.lock().await.take() {
            t.stop().await;
        }
        let all: Vec<_> = self.shared.live.lock().await.values().cloned().collect();
        for live in all {
            live.end(false).await;
        }
        for token in self
            .shared
            .checks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
        {
            token.cancel();
        }
    }
}

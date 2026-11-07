//! A real core service and a real relay on loopback, over a real in-memory store.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use fs_objects::TursoStore;
use fs_objects::traits::UnloggedObjects;
use fs_streaming_core::{ObjectId, StreamService, Streams};
use fs_streaming_relay::{FolderSink, RelayConfig, RtmpRelay};

static N: AtomicU32 = AtomicU32::new(0);

/// A scratch folder under `TMPDIR`, removed at the end.
pub struct Scratch(pub PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn scratch(name: &str) -> Scratch {
    let dir = std::env::temp_dir().join(format!(
        "fs-streaming-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    Scratch(dir)
}

pub struct Rig {
    pub service: Arc<StreamService>,
    pub relay: Arc<RtmpRelay>,
    pub addr: SocketAddr,
    pub store: Arc<dyn UnloggedObjects>,
}

/// A stream made through the service.
#[derive(Clone)]
pub struct Made {
    pub id: ObjectId,
    pub url: String,
    pub key: String,
}

impl Made {
    /// What ffmpeg is given.
    pub fn full_url(&self) -> String {
        format!("{}/{}", self.url, self.key)
    }
    /// Application and stream name as a plain client would send them.
    pub fn app_and_name(&self) -> (String, String) {
        (format!("live/{}", self.id), self.key.clone())
    }
}

impl Rig {
    pub async fn new() -> Rig {
        Rig::with(RelayConfig::new()).await
    }

    pub async fn recording(dir: &std::path::Path) -> Rig {
        Rig::with(RelayConfig::new().sink(Arc::new(FolderSink::new(dir)))).await
    }

    pub async fn with(cfg: RelayConfig) -> Rig {
        let store = Arc::new(TursoStore::open_in_memory().await.unwrap());
        Rig::with_store(store, cfg).await
    }

    pub async fn with_store(store: Arc<dyn UnloggedObjects>, cfg: RelayConfig) -> Rig {
        let service = StreamService::new(store.clone());
        let relay = RtmpRelay::start(service.clone(), cfg).await.unwrap();
        assert!(service.attach(relay.clone()));
        let addr = relay.local_addr();
        Rig {
            service,
            relay,
            addr,
            store,
        }
    }

    pub async fn stream(&self, name: &str) -> Made {
        let c = self.service.create(name).await.unwrap();
        Made {
            id: c.stream.id,
            url: c.ingest_url,
            key: c.publisher_key.expose().to_string(),
        }
    }

    /// A target that is another stream on another relay instance.
    pub async fn add_target_to(
        &self,
        from: &Made,
        to: &Made,
        name: &str,
    ) -> fs_streaming_core::Target {
        self.service
            .add_target(from.id, name, &to.url, &to.key)
            .await
            .unwrap()
    }
}

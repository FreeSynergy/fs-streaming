//! What every part of the relay shares.
//!
//! Pattern: Registry — the live streams by id and the running checks by stream id, plus the settings.

use std::collections::HashMap;
use std::sync::Arc;

use fs_streaming_core::{Gate, ObjectId};
use tokio::sync::Mutex as AsyncMutex;

use crate::config::RelayConfig;
use crate::live::LiveStream;
use crate::wire::{Dialer, Tls};

/// Shared state of one relay.
pub struct Shared {
    pub gate: Arc<dyn Gate>,
    pub cfg: RelayConfig,
    pub dialer: Dialer,
    pub live: AsyncMutex<HashMap<ObjectId, Arc<LiveStream>>>,
}

impl Shared {
    pub fn new(gate: Arc<dyn Gate>, cfg: RelayConfig) -> Arc<Self> {
        let dialer = Dialer::new(Tls::new(&cfg.extra_roots), cfg.limits);
        Arc::new(Self {
            gate,
            cfg,
            dialer,
            live: AsyncMutex::new(HashMap::new()),
        })
    }

    /// The live stream of `id`, made if there is none.
    pub async fn live_for(self: &Arc<Self>, id: ObjectId, name: &str) -> Arc<LiveStream> {
        let mut map = self.live.lock().await;
        map.entry(id)
            .or_insert_with(|| LiveStream::new(id, name.to_string(), self.clone()))
            .clone()
    }

    /// Forget a live stream, if it is still this one.
    pub async fn remove_live(&self, live: &Arc<LiveStream>) {
        let mut map = self.live.lock().await;
        if map.get(&live.id).is_some_and(|l| Arc::ptr_eq(l, live)) {
            map.remove(&live.id);
        }
    }
}

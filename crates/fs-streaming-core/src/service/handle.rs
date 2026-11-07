//! `StreamService`: construction, the relay link, and the views.
//!
//! Pattern: Facade — one small type hides the repo, the runtime state, the relay port and the feed.
//! Views turn rows plus runtime state into the types of the contract.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use fs_objects::ObjectId;
use fs_objects::traits::UnloggedObjects;
use tokio::sync::broadcast;

use super::runtime::StreamRt;
use crate::error::{Error, Result};
use crate::model::{Stream, StreamState, Target, TargetState};
use crate::repo::{Repo, StreamRow, TargetRow};
use crate::traits::{Event, Relay};
use crate::url::TargetUrl;

/// The streaming program core. Cheap to share through its `Arc`.
pub struct StreamService {
    pub(super) repo: Repo,
    relay: OnceLock<Arc<dyn Relay>>,
    runtime: Mutex<HashMap<ObjectId, StreamRt>>,
    pub(super) feed: broadcast::Sender<Event>,
}

impl StreamService {
    /// A service over a private store (kept apart from the hub and from logged data).
    pub fn new(store: Arc<dyn UnloggedObjects>) -> Arc<Self> {
        let (feed, _) = broadcast::channel(256);
        Arc::new(Self {
            repo: Repo::new(store),
            relay: OnceLock::new(),
            runtime: Mutex::new(HashMap::new()),
            feed,
        })
    }

    /// Attach the relay; once. Returns `false` if one is attached already.
    pub fn attach(&self, relay: Arc<dyn Relay>) -> bool {
        self.relay.set(relay).is_ok()
    }

    pub(super) fn relay(&self) -> Result<&Arc<dyn Relay>> {
        self.relay.get().ok_or(Error::NoRelay)
    }

    pub(super) fn rt(&self) -> MutexGuard<'_, HashMap<ObjectId, StreamRt>> {
        self.runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn emit(&self, event: Event) {
        let _ = self.feed.send(event);
    }

    pub(super) async fn row(&self, id: ObjectId) -> Result<StreamRow> {
        self.repo.get(id).await?.ok_or(Error::NotFound("stream"))
    }

    pub(super) fn state_of(&self, id: ObjectId) -> StreamState {
        self.rt().get(&id).map_or(StreamState::Idle, |r| r.state)
    }

    pub(super) fn ingest_url(&self, id: ObjectId) -> String {
        let base = self
            .relay
            .get()
            .map(|r| r.ingest_base())
            .unwrap_or_default();
        format!("{base}/{id}")
    }

    pub(super) fn stream_view(&self, row: &StreamRow) -> Stream {
        Stream {
            id: row.id,
            name: row.name.clone(),
            state: self.state_of(row.id),
            recording: row.recording,
            note: self.rt().get(&row.id).and_then(|r| r.note),
            ingest_url: self.ingest_url(row.id),
        }
    }

    pub(super) fn target_view(&self, stream: ObjectId, t: &TargetRow) -> Target {
        let live = self.rt().get(&stream).is_some_and(StreamRt::is_live);
        let known = self
            .rt()
            .get(&stream)
            .and_then(|r| r.targets.get(&t.id).copied());
        let (state, note) = known.map_or_else(
            || {
                let s = match (t.enabled, live) {
                    (false, _) => TargetState::Off,
                    (true, true) => TargetState::Connecting,
                    (true, false) => TargetState::Waiting,
                };
                (s, None)
            },
            |k| (k.state, k.note),
        );
        let url = TargetUrl::parse(&t.url);
        Target {
            id: t.id,
            stream,
            name: t.name.clone(),
            url: t.url.clone(),
            has_key: t.key.is_some(),
            enabled: t.enabled,
            secure: url.is_ok_and(|u| u.is_secure()),
            state,
            note,
        }
    }
}

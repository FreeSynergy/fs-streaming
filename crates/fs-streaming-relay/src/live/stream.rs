//! `LiveStream`: the life of one stream on the media side.
//!
//! Pattern: State + RAII — one lock guards the sender, the workers, the recorder and the grace timer.
//! The newest sender wins; a drop starts the grace timer (targets and recording stay open); the end
//! stops every task and tells the core. `ended` is final: a later sender gets a fresh `LiveStream`.

use std::collections::HashMap;
use std::sync::Arc;

use fs_streaming_core::{IngestGrant, ObjectId};
use tokio::sync::Mutex;

use super::worker::Worker;
use crate::fanout::Hub;
use crate::record::Recorder;
use crate::relay::Shared;
use crate::task::{CancelToken, Task};

/// A sender's place: the generation and the signal that ends it.
#[derive(Clone)]
pub struct Ticket {
    /// Which sender this is.
    pub serial: u64,
    /// Fires when the sender is replaced or stopped.
    pub cancel: CancelToken,
}

#[derive(Default)]
pub(super) struct Inner {
    pub(super) sender: Option<Ticket>,
    pub(super) serial: u64,
    pub(super) workers: HashMap<ObjectId, Worker>,
    pub(super) recorder: Option<Recorder>,
    pub(super) grace: Option<Task>,
    pub(super) running: bool,
    pub(super) ended: bool,
}

/// One stream while a sender is connected or interrupted.
pub struct LiveStream {
    /// The stream id.
    pub id: ObjectId,
    pub(super) name: String,
    /// Where its frames enter.
    pub hub: Arc<Hub>,
    pub(super) shared: Arc<Shared>,
    pub(super) inner: Mutex<Inner>,
}

impl LiveStream {
    /// A new, empty live stream.
    pub fn new(id: ObjectId, name: String, shared: Arc<Shared>) -> Arc<Self> {
        Arc::new(Self {
            id,
            name,
            hub: Arc::new(Hub::default()),
            shared,
            inner: Mutex::new(Inner::default()),
        })
    }

    /// Accept a sender; the previous one is ended. `None` if this stream just ended.
    pub async fn attach(&self, grant: &IngestGrant) -> Option<Ticket> {
        let mut g = self.inner.lock().await;
        if g.ended {
            return None;
        }
        if let Some(old) = g.sender.take() {
            old.cancel.cancel();
        }
        if let Some(t) = g.grace.take() {
            t.stop().await;
        }
        g.serial += 1;
        let ticket = Ticket {
            serial: g.serial,
            cancel: CancelToken::default(),
        };
        g.sender = Some(ticket.clone());
        self.shared.gate.sender_connected(self.id).await;
        if !g.running {
            g.running = true;
            for spec in grant.targets.iter().cloned() {
                g.workers
                    .insert(spec.id, Worker::start(spec, self.hub.clone(), &self.shared));
            }
            if grant.recording {
                self.start_recorder(&mut g).await;
            }
        }
        Some(ticket)
    }

    /// The sender with this generation is gone. Starts the grace time if it was the current one.
    pub async fn dropped(self: &Arc<Self>, serial: u64) {
        let mut g = self.inner.lock().await;
        if g.ended || g.sender.as_ref().map(|s| s.serial) != Some(serial) {
            return;
        }
        g.sender = None;
        self.shared.gate.sender_lost(self.id).await;
        let (me, grace) = (self.clone(), self.shared.cfg.grace);
        g.grace = Some(Task::spawn(async move {
            tokio::time::sleep(grace).await;
            me.end(true).await;
        }));
    }

    /// End everything: sender, workers, recording. Tells the core.
    pub async fn end(self: &Arc<Self>, from_grace: bool) {
        {
            let mut g = self.inner.lock().await;
            if g.ended {
                return;
            }
            g.ended = true;
            if let Some(s) = g.sender.take() {
                s.cancel.cancel();
            }
            if let Some(t) = g.grace.take() {
                if from_grace {
                    t.detach();
                } else {
                    t.stop().await;
                }
            }
            for (_, w) in g.workers.drain() {
                w.stop().await;
            }
            if let Some(r) = g.recorder.take() {
                r.finish().await;
            }
        }
        self.shared.remove_live(self).await;
        self.shared.gate.sender_ended(self.id).await;
    }
}

//! `Gate`: what the media side asks and reports.
//!
//! Pattern: Observer (the relay reports, the core applies the state functions and feeds listeners).
//! A wrong key and an unknown stream take the same path and give the same `None`.

use async_trait::async_trait;
use fs_objects::ObjectId;

use super::handle::StreamService;
use crate::keys;
use crate::model::StreamState;
use crate::state::{StreamEvent, TargetEvent, next_target};
use crate::traits::{Event, Gate, IngestGrant, RecordingEvent, RelayTargetEvent};

#[async_trait]
impl Gate for StreamService {
    async fn authorize(&self, stream: ObjectId, key: &str) -> Option<IngestGrant> {
        let (real, work) = self.repo.get_or_decoy(stream).await.ok()?;
        let ok = keys::matches(real.as_ref().map(|r| r.key_hash.as_str()), key);
        // The same work for a wrong key and an unknown stream: the grant is built either way.
        let row = work?;
        let targets = row
            .targets
            .iter()
            .filter(|t| t.enabled)
            .filter_map(|t| Self::spec(t).ok())
            .collect();
        let grant = IngestGrant {
            stream,
            name: row.name.clone(),
            recording: row.recording,
            targets,
        };
        (ok && real.is_some()).then_some(grant)
    }

    async fn listen_failed(&self) {
        let Ok(rows) = self.repo.all().await else {
            return;
        };
        for row in rows {
            {
                let mut guard = self.rt();
                let rt = guard.entry(row.id).or_default();
                if rt.apply(StreamEvent::Failed) == Ok(StreamState::Error) {
                    rt.note = Some(crate::note::Note::CannotListen);
                }
            }
            self.emit_all(row.id).await;
        }
    }

    async fn sender_connected(&self, stream: ObjectId) {
        let Ok(Some(row)) = self.repo.get(stream).await else {
            return;
        };
        {
            let mut guard = self.rt();
            let rt = guard.entry(stream).or_default();
            let was_live = rt.is_live();
            rt.generation += 1;
            if let Some(t) = rt.check_cancel.take() {
                t.cancel();
            }
            let _ = rt.apply(StreamEvent::SenderConnected);
            rt.note = None;
            if !was_live {
                for t in row.targets.iter().filter(|t| t.enabled) {
                    let cur = rt
                        .targets
                        .get(&t.id)
                        .map_or(crate::model::TargetState::Waiting, |k| k.state);
                    let state = next_target(cur, TargetEvent::StreamLive);
                    rt.targets
                        .insert(t.id, super::runtime::TargetRt { state, note: None });
                }
            }
        }
        self.emit_all(stream).await;
    }

    async fn sender_lost(&self, stream: ObjectId) {
        let _ = self
            .rt()
            .entry(stream)
            .or_default()
            .apply(StreamEvent::SenderLost);
        self.emit_all(stream).await;
    }

    async fn sender_ended(&self, stream: ObjectId) {
        self.finish_sender(stream, None).await;
    }

    async fn target_event(&self, target: ObjectId, event: RelayTargetEvent) {
        let Ok(Some((row, i))) = self.repo.find_target(target).await else {
            return;
        };
        let (ev, note) = match event {
            RelayTargetEvent::Connecting => (TargetEvent::Connecting, None),
            RelayTargetEvent::Sending => (TargetEvent::Connected, None),
            RelayTargetEvent::Lost(n) => (TargetEvent::Lost, Some(n)),
            RelayTargetEvent::Refused(n) => (TargetEvent::Refused, Some(n)),
        };
        let mut view = self.step_target(&row, i, ev);
        if let Some(k) = self
            .rt()
            .get_mut(&row.id)
            .and_then(|r| r.targets.get_mut(&target))
        {
            if note.is_some() || event == RelayTargetEvent::Sending {
                k.note = note;
            }
            view.note = k.note;
        }
        self.emit(Event::Target(view));
    }

    async fn recording_event(&self, stream: ObjectId, event: RecordingEvent) {
        match event {
            RecordingEvent::Problem(p) => {
                self.rt().entry(stream).or_default().problem = p;
            }
            RecordingEvent::Finished(info) => self.emit(Event::Recording(info)),
        }
    }
}

impl StreamService {
    /// The generation of the sender now, to be handed to `finish_sender` later.
    pub(super) fn generation(&self, id: ObjectId) -> u64 {
        self.rt().get(&id).map_or(0, |r| r.generation)
    }

    /// After a sender is gone for good: state `ended` and every enabled target waits.
    pub(super) async fn finish_sender(&self, id: ObjectId, seen: Option<u64>) {
        {
            let mut guard = self.rt();
            let Some(rt) = guard.get_mut(&id) else { return };
            // A sender accepted since `seen` was read is not the one that was stopped.
            if seen.is_some_and(|g| g != rt.generation) {
                return;
            }
            if rt.apply(StreamEvent::Ended) == Ok(StreamState::Ended) {
                for t in rt.targets.values_mut() {
                    t.state = next_target(t.state, TargetEvent::StreamEnded);
                }
            }
        }
        self.emit_all(id).await;
    }
}

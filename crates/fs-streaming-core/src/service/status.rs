//! `status`: what a stream is doing now, from the runtime state and the relay's counters.
//!
//! Pattern: Facade view — nothing here is stored; it is put together on each call.

use fs_objects::ObjectId;

use super::handle::StreamService;
use crate::error::Result;
use crate::model::{Status, TargetStatus};

impl StreamService {
    pub(super) async fn status_inner(&self, id: ObjectId) -> Result<Status> {
        let row = self.row(id).await?;
        let seen = match self.relay() {
            Ok(r) => r.status(id).await.unwrap_or_default(),
            Err(_) => crate::traits::RelayStatus::default(),
        };
        let rt = self.rt().get(&id).cloned().unwrap_or_default();
        let targets = row
            .targets
            .iter()
            .map(|t| {
                let v = self.target_view(id, t);
                let c = seen
                    .targets
                    .iter()
                    .find(|(i, _)| *i == t.id)
                    .map(|(_, c)| *c)
                    .unwrap_or_default();
                TargetStatus {
                    target: t.id,
                    state: v.state,
                    bytes_out: c.bytes_out,
                    dropped: c.dropped,
                    reconnects: c.reconnects,
                    note: v.note,
                }
            })
            .collect();
        Ok(Status {
            state: rt.state,
            since: rt.since,
            bytes_in: seen.bytes_in,
            targets,
            has_video: seen.has_video,
            has_audio: seen.has_audio,
            recording_problem: rt.problem,
        })
    }
}

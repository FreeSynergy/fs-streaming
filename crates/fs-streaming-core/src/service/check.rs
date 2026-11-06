//! `check`: try the enabled targets without sending media.
//!
//! Pattern: Command with a guard — the state function refuses while live; the state goes to
//! `checking` and back to where it came from, also when `stop` or a sender cut the check short.

use fs_store::ObjectId;

use super::handle::StreamService;
use crate::error::{Error, Result};
use crate::model::{CheckResult, StreamState};
use crate::state::StreamEvent;

impl StreamService {
    pub(super) async fn check_inner(&self, stream: ObjectId) -> Result<Vec<CheckResult>> {
        let relay = self.relay()?.clone();
        let row = self.row(stream).await?;
        self.rt()
            .entry(stream)
            .or_default()
            .apply(StreamEvent::CheckStarted)
            .map_err(Error::Refused)?;
        self.emit_all(stream).await;
        let specs = row
            .targets
            .iter()
            .filter(|t| t.enabled)
            .filter_map(|t| Self::spec(t).ok())
            .collect();
        let results = relay.check(stream, specs).await;
        self.finish_check(stream).await;
        Ok(results)
    }

    /// Leave `checking` for where the stream came from; nothing if it is not checking.
    pub(super) async fn finish_check(&self, stream: ObjectId) {
        let changed = {
            let mut guard = self.rt();
            guard.get_mut(&stream).is_some_and(|rt| {
                let back = rt.before_check;
                rt.state == StreamState::Checking && rt.apply(StreamEvent::CheckEnded(back)).is_ok()
            })
        };
        if changed {
            self.emit_all(stream).await;
        }
    }
}

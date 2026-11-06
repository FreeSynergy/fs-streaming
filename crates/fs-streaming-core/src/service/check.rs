//! `check`: try the enabled targets without sending media.
//!
//! Pattern: Command with a guard — the state function refuses while live; the state goes to
//! `checking` and back to where it came from, also when `stop` or a sender cut the check short.

use fs_store::ObjectId;

use super::handle::StreamService;
use crate::cancel::CancelToken;
use crate::error::{Error, Result};
use crate::model::{CheckResult, StreamState};
use crate::state::StreamEvent;

impl StreamService {
    pub(super) async fn check_inner(&self, stream: ObjectId) -> Result<Vec<CheckResult>> {
        let relay = self.relay()?.clone();
        let row = self.row(stream).await?;
        let cancel = CancelToken::default();
        {
            let mut guard = self.rt();
            let rt = guard.entry(stream).or_default();
            rt.apply(StreamEvent::CheckStarted)
                .map_err(Error::Refused)?;
            rt.check_cancel = Some(cancel.clone());
        }
        self.emit_all(stream).await;
        let specs = row
            .targets
            .iter()
            .filter(|t| t.enabled)
            .filter_map(|t| Self::spec(t).ok())
            .collect();
        let results = relay.check(specs, cancel).await;
        self.finish_check(stream).await;
        Ok(results)
    }

    /// Leave `checking` for where the stream came from; nothing if it is not checking.
    pub(super) async fn finish_check(&self, stream: ObjectId) {
        let changed = {
            let mut guard = self.rt();
            guard.get_mut(&stream).is_some_and(|rt| {
                rt.check_cancel = None;
                let back = rt.before_check;
                rt.state == StreamState::Checking && rt.apply(StreamEvent::CheckEnded(back)).is_ok()
            })
        };
        if changed {
            self.emit_all(stream).await;
        }
    }
}

impl StreamService {
    /// Cancel the running check of a stream, if there is one.
    pub(super) fn cancel_check(&self, stream: ObjectId) {
        if let Some(t) = self.rt().get(&stream).and_then(|r| r.check_cancel.clone()) {
            t.cancel();
        }
    }
}

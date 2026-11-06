//! What is only true while the program runs: states, times, problems.
//!
//! Pattern: State holder — the transition functions in `state` decide; this file only keeps the
//! current values, so nothing of it is stored.

use std::collections::HashMap;

use fs_store::{ObjectId, Timestamp};

use crate::model::{StreamState, TargetState};
use crate::note::Note;
use crate::state::{Refusal, StreamEvent, next_stream};

/// Runtime of one target.
#[derive(Clone, Copy, Debug)]
pub(super) struct TargetRt {
    pub state: TargetState,
    pub note: Option<Note>,
}

/// Runtime of one stream.
#[derive(Clone, Debug)]
pub(super) struct StreamRt {
    pub state: StreamState,
    pub since: Timestamp,
    /// Where a check came from.
    pub before_check: StreamState,
    pub problem: Option<Note>,
    pub targets: HashMap<ObjectId, TargetRt>,
}

impl Default for StreamRt {
    fn default() -> Self {
        Self {
            state: StreamState::Idle,
            since: chrono::Utc::now(),
            before_check: StreamState::Idle,
            problem: None,
            targets: HashMap::new(),
        }
    }
}

impl StreamRt {
    /// Apply an event; remember where a check came from; stamp the time on a change.
    pub fn apply(&mut self, event: StreamEvent) -> Result<StreamState, Refusal> {
        let next = next_stream(self.state, event)?;
        if event == StreamEvent::CheckStarted {
            self.before_check = self.state;
        }
        if next != self.state {
            self.state = next;
            self.since = chrono::Utc::now();
        }
        Ok(next)
    }

    /// Whether senders are (or were a moment ago) connected.
    pub fn is_live(&self) -> bool {
        matches!(self.state, StreamState::Live | StreamState::Interrupted)
    }
}

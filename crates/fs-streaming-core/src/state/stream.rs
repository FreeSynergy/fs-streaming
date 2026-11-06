//! Transition function of a stream.
//!
//! Pattern: State — `next_stream` is the only place that decides what follows what. `idle` and `ended`
//! both accept a sender; `interrupted` becomes `ended` only after the grace time; `check` and `delete`
//! are refused while a sender could be hurt.

use std::fmt;

use crate::model::StreamState;

/// What happened to a stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEvent {
    /// A sender was accepted (the newest wins, so this is valid in every state).
    SenderConnected,
    /// The sender dropped.
    SenderLost,
    /// `stop`, `rotateKey`, or the grace time after a drop ran out.
    Ended,
    /// A check starts.
    CheckStarted,
    /// The check is over; the state it came from.
    CheckEnded(StreamState),
    /// `delete` is asked for.
    DeleteAsked,
}

/// Why a state refuses an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// Delete while live, interrupted or checking.
    StreamBusy,
    /// Check while live or interrupted.
    CheckWhileLive,
    /// Check while another check runs.
    CheckRunning,
    /// Edit of a target that is sending.
    TargetSending,
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::StreamBusy => "the stream is live, interrupted or being checked",
            Self::CheckWhileLive => {
                "a check would use a second connection that could end the real one"
            }
            Self::CheckRunning => "a check is already running",
            Self::TargetSending => "the target is sending; switch it off first",
        })
    }
}

/// The state after `event`, or the refusal.
#[allow(clippy::match_same_arms)] // one rule per line reads better than merged patterns
pub fn next_stream(state: StreamState, event: StreamEvent) -> Result<StreamState, Refusal> {
    use StreamState::{Checking, Ended, Error, Idle, Interrupted, Live};
    Ok(match (state, event) {
        (_, StreamEvent::SenderConnected) => Live,
        (Live, StreamEvent::SenderLost) => Interrupted,
        (Live | Interrupted, StreamEvent::Ended) => Ended,
        (Idle | Ended | Error, StreamEvent::CheckStarted) => Checking,
        (Checking, StreamEvent::CheckStarted) => return Err(Refusal::CheckRunning),
        (Live | Interrupted, StreamEvent::CheckStarted) => return Err(Refusal::CheckWhileLive),
        (Checking, StreamEvent::CheckEnded(back)) => back,
        (Live | Interrupted | Checking, StreamEvent::DeleteAsked) => {
            return Err(Refusal::StreamBusy);
        }
        (s, _) => s,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use StreamState::{Checking, Ended, Idle, Interrupted, Live};

    #[test]
    fn idle_and_ended_accept_a_sender() {
        assert_eq!(next_stream(Idle, StreamEvent::SenderConnected), Ok(Live));
        assert_eq!(next_stream(Ended, StreamEvent::SenderConnected), Ok(Live));
        assert_eq!(
            next_stream(Checking, StreamEvent::SenderConnected),
            Ok(Live)
        );
    }

    #[test]
    fn a_drop_is_interrupted_and_only_the_grace_ends_it() {
        assert_eq!(next_stream(Live, StreamEvent::SenderLost), Ok(Interrupted));
        assert_eq!(
            next_stream(Interrupted, StreamEvent::SenderConnected),
            Ok(Live)
        );
        assert_eq!(next_stream(Interrupted, StreamEvent::Ended), Ok(Ended));
        assert_eq!(next_stream(Idle, StreamEvent::Ended), Ok(Idle));
    }

    #[test]
    fn refusals() {
        assert_eq!(
            next_stream(Live, StreamEvent::CheckStarted),
            Err(Refusal::CheckWhileLive)
        );
        assert_eq!(
            next_stream(Interrupted, StreamEvent::CheckStarted),
            Err(Refusal::CheckWhileLive)
        );
        assert_eq!(
            next_stream(Checking, StreamEvent::CheckStarted),
            Err(Refusal::CheckRunning)
        );
        for s in [Live, Interrupted, Checking] {
            assert_eq!(
                next_stream(s, StreamEvent::DeleteAsked),
                Err(Refusal::StreamBusy)
            );
        }
        assert_eq!(next_stream(Idle, StreamEvent::DeleteAsked), Ok(Idle));
        assert_eq!(next_stream(Ended, StreamEvent::DeleteAsked), Ok(Ended));
    }

    #[test]
    fn a_check_returns_to_where_it_came_from() {
        assert_eq!(next_stream(Idle, StreamEvent::CheckStarted), Ok(Checking));
        assert_eq!(
            next_stream(Checking, StreamEvent::CheckEnded(Ended)),
            Ok(Ended)
        );
        assert_eq!(next_stream(Live, StreamEvent::CheckEnded(Idle)), Ok(Live));
    }
}

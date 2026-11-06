//! Transition function of a target.
//!
//! Pattern: State — `next_target` decides what follows what. A switched-off target ignores everything
//! the relay still reports. `refused` waits for an edit, a switch-on, or the next live.

use crate::model::TargetState;

/// What happened to a target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetEvent {
    /// Switched on; whether the stream is live now.
    Enabled {
        /// The stream is live (or interrupted) at this moment.
        live: bool,
    },
    /// Switched off.
    Disabled,
    /// The stream went live.
    StreamLive,
    /// The stream ended.
    StreamEnded,
    /// The worker is connecting.
    Connecting,
    /// The worker is sending.
    Connected,
    /// The connection was lost.
    Lost,
    /// The far end said no explicitly.
    Refused,
    /// The address was edited; whether the stream is live now.
    Edited {
        /// The stream is live (or interrupted) at this moment.
        live: bool,
    },
}

/// The state after `event`.
#[allow(clippy::match_same_arms)] // one rule per line reads better than merged patterns
pub fn next_target(state: TargetState, event: TargetEvent) -> TargetState {
    use TargetState::{Connecting, Off, Refused, Retrying, Sending, Waiting};
    match (state, event) {
        (_, TargetEvent::Disabled) => Off,
        (Off | Refused, TargetEvent::Enabled { live: true }) => Connecting,
        (Off | Refused, TargetEvent::Enabled { live: false }) => Waiting,
        (Refused, TargetEvent::Edited { live: true }) => Connecting,
        (Refused, TargetEvent::Edited { live: false }) => Waiting,
        (Off, _) => Off,
        (Refused, TargetEvent::Lost | TargetEvent::Connecting | TargetEvent::Connected) => Refused,
        (Waiting | Refused | Retrying, TargetEvent::StreamLive) => Connecting,
        (_, TargetEvent::StreamEnded) => Waiting,
        (_, TargetEvent::Connecting) => Connecting,
        (_, TargetEvent::Connected) => Sending,
        (_, TargetEvent::Lost) => Retrying,
        (_, TargetEvent::Refused) => Refused,
        (s, _) => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use TargetState::{Connecting, Off, Refused, Retrying, Sending, Waiting};

    #[test]
    fn the_normal_life() {
        let s = next_target(Off, TargetEvent::Enabled { live: false });
        assert_eq!(s, Waiting);
        let s = next_target(s, TargetEvent::StreamLive);
        assert_eq!(s, Connecting);
        let s = next_target(s, TargetEvent::Connected);
        assert_eq!(s, Sending);
        let s = next_target(s, TargetEvent::Lost);
        assert_eq!(s, Retrying);
        assert_eq!(next_target(s, TargetEvent::Connecting), Connecting);
        assert_eq!(next_target(Sending, TargetEvent::StreamEnded), Waiting);
    }

    #[test]
    fn refused_waits_for_a_reason_to_try_again() {
        assert_eq!(next_target(Connecting, TargetEvent::Refused), Refused);
        assert_eq!(next_target(Refused, TargetEvent::Lost), Refused);
        assert_eq!(next_target(Refused, TargetEvent::StreamLive), Connecting);
        assert_eq!(
            next_target(Refused, TargetEvent::Edited { live: false }),
            Waiting
        );
        assert_eq!(
            next_target(Refused, TargetEvent::Enabled { live: true }),
            Connecting
        );
    }

    #[test]
    fn off_ignores_late_reports() {
        for e in [
            TargetEvent::Connected,
            TargetEvent::Lost,
            TargetEvent::Refused,
            TargetEvent::StreamLive,
        ] {
            assert_eq!(next_target(Off, e), Off);
        }
        assert_eq!(next_target(Sending, TargetEvent::Disabled), Off);
    }
}

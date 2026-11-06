//! Acknowledgements and write limits of a sender connection.
//!
//! Pattern: counter + guard — the relay counts the bytes it really read and acknowledges them as often
//! as the sender's window asks, so a sender that waits for acknowledgements does not stall. Until the
//! sender has published, a write must also finish within the publish deadline.

use std::time::{Duration, Instant};

use super::conn::{Conn, write};
use crate::error::WireError;

/// Acknowledgement bookkeeping (RTMP type 3), counted on the real socket bytes.
#[derive(Default)]
pub struct Acks {
    pub read: u64,
    pub window: u64,
    pub sent: u64,
}

impl Conn<'_> {
    /// How long a write may take: until the sender has published, the 10 s deadline applies to it too.
    pub fn write_limit(&self) -> Duration {
        let stall = self.shared.cfg.limits.write_stall;
        if self.publishing.is_some() {
            stall
        } else {
            stall.min(self.deadline.saturating_duration_since(Instant::now()))
        }
    }

    /// Count bytes read and acknowledge them as often as the sender's window asks.
    pub async fn acknowledge(&mut self, n: usize) -> Result<(), WireError> {
        self.acks.read += n as u64;
        while self.acks.window > 0 && self.acks.read - self.acks.sent >= self.acks.window {
            self.acks.sent = self.acks.read;
            // Type 3 on chunk stream 2, written whole: four bytes, the count so far.
            let mut m = vec![0x02, 0, 0, 0, 0, 0, 4, 3, 0, 0, 0, 0];
            m.extend_from_slice(&((self.acks.read & 0xffff_ffff) as u32).to_be_bytes());
            let stall = self.write_limit();
            write(self.io, &m, stall).await?;
        }
        Ok(())
    }
}

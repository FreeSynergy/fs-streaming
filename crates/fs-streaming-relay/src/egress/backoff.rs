//! Growing pauses between tries.
//!
//! Pattern: Iterator-like value object — the pause doubles from `min` to `max`, with up to 20 percent
//! jitter so many targets do not retry together.

use std::time::Duration;

/// Pauses 1 s, 2 s, 4 s … up to a maximum (the numbers come from the config).
#[derive(Clone, Debug)]
pub struct Backoff {
    min: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    /// From `min` to `max`.
    pub fn new(min: Duration, max: Duration) -> Self {
        Self {
            min,
            max,
            attempt: 0,
        }
    }

    /// The pause before jitter.
    pub fn base(&self) -> Duration {
        self.min
            .saturating_mul(1u32 << self.attempt.min(20))
            .min(self.max)
    }

    /// The next pause (and move on).
    pub fn next_pause(&mut self) -> Duration {
        let base = self.base();
        self.attempt = self.attempt.saturating_add(1);
        let mut r = [0u8; 2];
        let _ = getrandom::fill(&mut r);
        let jitter = 0.8 + 0.4 * f64::from(u16::from_be_bytes(r)) / f64::from(u16::MAX);
        base.mul_f64(jitter)
    }

    /// Start over after a good connection.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_to_the_maximum_and_resets() {
        let mut b = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
        let bases: Vec<_> = (0..8)
            .map(|_| {
                let x = b.base();
                b.next_pause();
                x.as_secs()
            })
            .collect();
        assert_eq!(bases, vec![1, 2, 4, 8, 16, 32, 60, 60]);
        b.reset();
        assert_eq!(b.base().as_secs(), 1);
    }

    #[test]
    fn jitter_stays_within_twenty_percent() {
        for _ in 0..50 {
            let p = Backoff::new(Duration::from_secs(10), Duration::from_secs(60)).next_pause();
            assert!(p >= Duration::from_secs(8) && p <= Duration::from_secs(12));
        }
    }
}

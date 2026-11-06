//! Waiting for a condition instead of sleeping a fixed time.

use std::future::Future;
use std::time::{Duration, Instant};

/// Poll `check` until it gives `Some`, or panic after `limit`.
pub async fn until<T, F, Fut>(what: &str, limit: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = Instant::now();
    loop {
        if let Some(v) = check().await {
            return v;
        }
        assert!(start.elapsed() < limit, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The usual limit on this loaded machine.
pub const LONG: Duration = Duration::from_secs(30);

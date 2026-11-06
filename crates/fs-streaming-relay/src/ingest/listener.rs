//! The accept loop.
//!
//! Pattern: Supervisor — every connection runs in a `JoinSet` owned by this task, so stopping the loop
//! stops all connections that are not tied to a live stream any more. At most `max_connections` at
//! once; beyond that a new connection is closed at once. An accept error (out of descriptors) is
//! followed by a pause, so the loop never spins.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::session::serve;
use crate::relay::Shared;

/// Accept until the task is aborted.
pub async fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    let mut set = JoinSet::new();
    let slots = Arc::new(Semaphore::new(shared.cfg.limits.max_connections));
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((socket, _)) => match slots.clone().try_acquire_owned() {
                    Ok(permit) => { set.spawn(serve(shared.clone(), socket, permit)); }
                    Err(_) => drop(socket),
                },
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            },
            Some(_) = set.join_next(), if !set.is_empty() => {}
        }
    }
}

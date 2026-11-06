//! The accept loop.
//!
//! Pattern: Supervisor — every connection runs in a `JoinSet` owned by this task, so stopping the loop
//! stops all connections that are not tied to a live stream any more.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinSet;

use super::session::serve;
use crate::relay::Shared;

/// Accept until the task is aborted.
pub async fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    let mut set = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                if let Ok((socket, _)) = accepted {
                    set.spawn(serve(shared.clone(), socket));
                }
            }
            Some(_) = set.join_next(), if !set.is_empty() => {}
        }
    }
}

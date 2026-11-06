//! One sender's connection.
//!
//! Pattern: State machine over `rml_rtmp`'s `ServerSession`, which only ever sees commands and control
//! messages (re-chunked by us). Media never goes through it, because it re-writes metadata and cannot
//! be trusted with AMF it did not need to read. Every message is handled in order, and only media on
//! the published stream id, after `publish`, is forwarded. A wrong key and an unknown stream get the
//! same refusal. Limits, deadlines and panics end this connection only.

use std::time::Instant;

use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::handshake::PeerType;
use rml_rtmp::sessions::{ServerSession, ServerSessionConfig, ServerSessionResult};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;

use std::sync::Arc;

use super::acks::Acks;
use super::conn::Conn;
use super::publishing::Publishing;
use crate::error::WireError;
use crate::relay::Shared;
use crate::task::CancelToken;
use crate::wire::{MessageReader, handshake};

/// Serve one connection until it ends; tell the live stream if the sender was its current one.
pub async fn serve(shared: Arc<Shared>, mut io: TcpStream, permit: OwnedSemaphorePermit) {
    let _ = io.set_nodelay(true);
    let cancel = CancelToken::default();
    let mut publishing = None;
    let run = run(&shared, &mut io, cancel.clone(), &mut publishing, permit);
    let outcome = tokio::select! {
        r = run => r,
        () = cancel.cancelled() => Ok(()),
    };
    if let Err(e) = outcome {
        tracing::debug!("sender connection ended: {e}");
    }
    if let Some(p) = publishing {
        p.release().await;
    }
}

async fn run(
    shared: &Arc<Shared>,
    io: &mut TcpStream,
    cancel: CancelToken,
    publishing: &mut Option<Publishing>,
    permit: OwnedSemaphorePermit,
) -> Result<(), WireError> {
    let lim = shared.cfg.limits;
    let deadline = Instant::now() + lim.publish_deadline;
    let mut early = handshake(
        io,
        PeerType::Server,
        lim.handshake_timeout.min(lim.publish_deadline),
    )
    .await?;
    let (session, first) =
        ServerSession::new(ServerSessionConfig::new()).map_err(|_| WireError::Protocol)?;
    let mut c = Conn {
        shared,
        io,
        cancel,
        session,
        ser: ChunkSerializer::new(),
        publishing: publishing.take(),
        permit: Some(permit),
        deadline,
        acks: Acks::default(),
    };
    let result = serve_messages(&mut c, &mut early, deadline, first).await;
    *publishing = c.publishing.take();
    result
}

async fn serve_messages(
    c: &mut Conn<'_>,
    early: &mut Vec<u8>,
    deadline: Instant,
    first: Vec<ServerSessionResult>,
) -> Result<(), WireError> {
    let lim = c.shared.cfg.limits;
    c.respond(first).await?;
    let mut reader = MessageReader::new(lim);
    let (mut buf, mut last_message) = (vec![0u8; 32 * 1024], Instant::now());
    loop {
        let data = if early.is_empty() {
            let now = Instant::now();
            let idle = lim
                .idle_timeout
                .saturating_sub(now.duration_since(last_message));
            let wait = if c.publishing.is_some() {
                idle
            } else {
                idle.min(deadline.saturating_duration_since(now))
            };
            if wait.is_zero() {
                return Err(WireError::Timeout);
            }
            let n = match tokio::time::timeout(wait, c.io.read(&mut buf)).await {
                Ok(Ok(0)) => return Err(WireError::Closed),
                Ok(Ok(n)) => n,
                Ok(Err(_)) => return Err(WireError::Io),
                Err(_) => return Err(WireError::Timeout),
            };
            buf[..n].to_vec()
        } else {
            std::mem::take(early)
        };
        if let Some(p) = &c.publishing {
            p.live.hub.count_in(data.len());
        }
        c.acknowledge(data.len()).await?;
        for m in reader.feed(&data)? {
            last_message = Instant::now();
            if !c.message(m).await? {
                return Ok(());
            }
        }
    }
}

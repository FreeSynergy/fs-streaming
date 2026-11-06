//! One sender's connection.
//!
//! Pattern: State machine over `rml_rtmp`'s `ServerSession` — the session answers `connect`,
//! `createStream` and `publish`; a second reader (`MessageReader`) over the same bytes gives the
//! media messages untouched, because the session would re-write metadata. A wrong key and an unknown
//! stream get the same refusal. Limits apply before anything is believed.

use std::sync::Arc;

use fs_streaming_core::ObjectId;
use rml_rtmp::handshake::PeerType;
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::path::parse;
use crate::error::WireError;
use crate::frame::{Frame, Kind};
use crate::live::{LiveStream, Ticket};
use crate::relay::Shared;
use crate::wire::{MessageReader, handshake};

struct Publishing {
    live: Arc<LiveStream>,
    ticket: Ticket,
}

/// Serve one connection until it ends; tell the live stream if the sender was its current one.
pub async fn serve(shared: Arc<Shared>, mut io: TcpStream) {
    let _ = io.set_nodelay(true);
    let mut publishing = None;
    let outcome = run(&shared, &mut io, &mut publishing).await;
    if let Err(e) = outcome {
        tracing::debug!("sender connection ended: {e}");
    }
    if let Some(p) = publishing {
        p.live.dropped(p.ticket.serial).await;
    }
}

async fn send(
    io: &mut TcpStream,
    results: Vec<ServerSessionResult>,
) -> Result<Vec<ServerSessionEvent>, WireError> {
    let mut events = Vec::new();
    for r in results {
        match r {
            ServerSessionResult::OutboundResponse(p) => io.write_all(&p.bytes).await?,
            ServerSessionResult::RaisedEvent(e) => events.push(e),
            ServerSessionResult::UnhandleableMessageReceived(_) => {}
        }
    }
    Ok(events)
}

async fn run(
    shared: &Arc<Shared>,
    io: &mut TcpStream,
    publishing: &mut Option<Publishing>,
) -> Result<(), WireError> {
    let lim = shared.cfg.limits;
    let mut early = handshake(io, PeerType::Server, lim.handshake_timeout).await?;
    let (mut session, first) =
        ServerSession::new(ServerSessionConfig::new()).map_err(|_| WireError::Protocol)?;
    send(io, first).await?;
    let mut reader = MessageReader::new(lim);
    let mut buf = vec![0u8; 32 * 1024];
    loop {
        let data = if early.is_empty() {
            let cancel = publishing.as_ref().map(|p| p.ticket.cancel.clone());
            let stopped = async {
                match cancel {
                    Some(c) => c.cancelled().await,
                    None => std::future::pending().await,
                }
            };
            let n = tokio::select! {
                r = tokio::time::timeout(lim.idle_timeout, io.read(&mut buf)) => match r {
                    Ok(Ok(0)) => return Err(WireError::Closed),
                    Ok(Ok(n)) => n,
                    Ok(Err(_)) => return Err(WireError::Io),
                    Err(_) => return Err(WireError::Timeout),
                },
                () = stopped => return Ok(()),
            };
            buf[..n].to_vec()
        } else {
            std::mem::take(&mut early)
        };
        if let Some(p) = publishing.as_ref() {
            p.live.hub.count_in(data.len());
        }
        let media = reader.feed(&data)?;
        let results = session
            .handle_input(&data)
            .map_err(|_| WireError::Protocol)?;
        for event in send(io, results).await? {
            if !on_event(shared, io, &mut session, publishing, event).await? {
                return Ok(());
            }
        }
        if let Some(p) = publishing.as_ref() {
            for m in media {
                if let Some(kind) = Kind::from_type_id(m.type_id) {
                    p.live
                        .hub
                        .publish(Frame::new(kind, m.timestamp.value, m.data));
                }
            }
        }
    }
}

/// `Ok(false)` ends the connection cleanly.
async fn on_event(
    shared: &Arc<Shared>,
    io: &mut TcpStream,
    session: &mut ServerSession,
    publishing: &mut Option<Publishing>,
    event: ServerSessionEvent,
) -> Result<bool, WireError> {
    match event {
        ServerSessionEvent::ConnectionRequested { request_id, .. } => {
            let out = session
                .accept_request(request_id)
                .map_err(|_| WireError::Protocol)?;
            send(io, out).await?;
        }
        ServerSessionEvent::PublishStreamRequested {
            request_id,
            app_name,
            stream_key,
            ..
        } => {
            let grant = match (parse(&app_name, &stream_key), publishing.is_some()) {
                (Some((id, key)), false) => shared.gate.authorize(id, &key).await.map(|g| (id, g)),
                _ => None,
            };
            let Some((id, grant)) = grant else {
                let out = session
                    .reject_request(request_id, "NetStream.Publish.BadName", "Publish refused.")
                    .map_err(|_| WireError::Protocol)?;
                send(io, out).await?;
                return Ok(false);
            };
            let Some((live, ticket)) = attach(shared, id, &grant).await else {
                return Ok(false);
            };
            *publishing = Some(Publishing { live, ticket });
            let out = session
                .accept_request(request_id)
                .map_err(|_| WireError::Protocol)?;
            send(io, out).await?;
        }
        ServerSessionEvent::PublishStreamFinished { .. } => return Ok(false),
        _ => {}
    }
    Ok(true)
}

async fn attach(
    shared: &Arc<Shared>,
    id: ObjectId,
    grant: &fs_streaming_core::IngestGrant,
) -> Option<(Arc<LiveStream>, Ticket)> {
    for _ in 0..3 {
        let live = shared.live_for(id, &grant.name).await;
        if let Some(ticket) = live.attach(grant).await {
            return Some((live, ticket));
        }
        shared.remove_live(&live).await;
    }
    None
}

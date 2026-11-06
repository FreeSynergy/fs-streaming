//! One sender's connection.
//!
//! Pattern: State machine over `rml_rtmp`'s `ServerSession`, which only ever sees commands and control
//! messages (re-chunked by us). Media never goes through it, because it re-writes metadata and cannot
//! be trusted with AMF it did not need to read. Every message is handled in order, and only media on
//! the published stream id, after `publish`, is forwarded. A wrong key and an unknown stream get the
//! same refusal. Limits, deadlines and panics end this connection only.

use std::sync::Arc;
use std::time::{Duration, Instant};

use fs_streaming_core::ObjectId;
use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::handshake::PeerType;
use rml_rtmp::messages::MessagePayload;
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;

use super::path::parse;
use crate::error::WireError;
use crate::frame::{Frame, Kind};
use crate::live::{Attach, LiveStream};
use crate::relay::Shared;
use crate::task::CancelToken;
use crate::wire::{MessageReader, handshake};

/// The stream this connection feeds. Dropping it without `release` still tells the live stream.
struct Publishing {
    live: Arc<LiveStream>,
    serial: u64,
    stream_id: u32,
    armed: bool,
}

impl Publishing {
    async fn release(mut self) {
        self.armed = false;
        self.live.dropped(self.serial).await;
    }
}

impl Drop for Publishing {
    fn drop(&mut self) {
        if self.armed {
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                let (live, serial) = (self.live.clone(), self.serial);
                rt.spawn(async move { live.dropped(serial).await });
            }
        }
    }
}

struct Conn<'a> {
    shared: &'a Arc<Shared>,
    io: &'a mut TcpStream,
    cancel: CancelToken,
    session: ServerSession,
    ser: ChunkSerializer,
    publishing: Option<Publishing>,
}

/// Serve one connection until it ends; tell the live stream if the sender was its current one.
pub async fn serve(shared: Arc<Shared>, mut io: TcpStream, _permit: OwnedSemaphorePermit) {
    let _ = io.set_nodelay(true);
    let cancel = CancelToken::default();
    let mut publishing = None;
    let run = run(&shared, &mut io, cancel.clone(), &mut publishing);
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

async fn write(io: &mut TcpStream, bytes: &[u8], stall: Duration) -> Result<(), WireError> {
    tokio::time::timeout(stall, io.write_all(bytes))
        .await
        .map_err(|_| WireError::Timeout)??;
    Ok(())
}

async fn run(
    shared: &Arc<Shared>,
    io: &mut TcpStream,
    cancel: CancelToken,
    publishing: &mut Option<Publishing>,
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
        for m in reader.feed(&data)? {
            last_message = Instant::now();
            if !c.message(m).await? {
                return Ok(());
            }
        }
    }
}

impl Conn<'_> {
    async fn respond(
        &mut self,
        results: Vec<ServerSessionResult>,
    ) -> Result<Vec<ServerSessionEvent>, WireError> {
        let mut events = Vec::new();
        for r in results {
            match r {
                ServerSessionResult::OutboundResponse(p) => {
                    write(self.io, &p.bytes, self.shared.cfg.limits.write_stall).await?;
                }
                ServerSessionResult::RaisedEvent(e) => events.push(e),
                ServerSessionResult::UnhandleableMessageReceived(_) => {}
            }
        }
        Ok(events)
    }

    /// One message, in order. `Ok(false)` ends the connection cleanly.
    async fn message(&mut self, m: MessagePayload) -> Result<bool, WireError> {
        match m.type_id {
            2..=6 | 17 | 20 => {
                let stream = m.message_stream_id;
                let chunks = self
                    .ser
                    .serialize(&m, true, false)
                    .map_err(|_| WireError::Protocol)?
                    .bytes;
                let session = &mut self.session;
                let results = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    session.handle_input(&chunks)
                }))
                .map_err(|_| WireError::Protocol)?
                .map_err(|_| WireError::Protocol)?;
                for event in self.respond(results).await? {
                    if !self.event(event, stream).await? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            8 | 9 | 18 => {
                if let (Some(p), Some(kind)) = (&self.publishing, Kind::from_type_id(m.type_id)) {
                    if p.stream_id == m.message_stream_id {
                        p.live
                            .hub
                            .publish(Frame::new(kind, m.timestamp.value, m.data));
                    }
                }
                Ok(true)
            }
            // SetChunkSize is the reader's business; other types are not for us.
            _ => Ok(true),
        }
    }

    async fn event(&mut self, event: ServerSessionEvent, stream: u32) -> Result<bool, WireError> {
        match event {
            ServerSessionEvent::ConnectionRequested { request_id, .. } => {
                let out = self
                    .session
                    .accept_request(request_id)
                    .map_err(|_| WireError::Protocol)?;
                self.respond(out).await?;
            }
            ServerSessionEvent::PublishStreamRequested {
                request_id,
                app_name,
                stream_key,
                ..
            } => {
                let attached = match (parse(&app_name, &stream_key), self.publishing.is_some()) {
                    (Some((id, key)), false) => self.attach(id, &key).await,
                    _ => None,
                };
                let Some(p) = attached else {
                    let out = self
                        .session
                        .reject_request(request_id, "NetStream.Publish.BadName", "Publish refused.")
                        .map_err(|_| WireError::Protocol)?;
                    self.respond(out).await?;
                    return Ok(false);
                };
                let mut p = p;
                p.stream_id = stream;
                self.publishing = Some(p);
                let out = self
                    .session
                    .accept_request(request_id)
                    .map_err(|_| WireError::Protocol)?;
                self.respond(out).await?;
            }
            ServerSessionEvent::PublishStreamFinished { .. } => return Ok(false),
            _ => {}
        }
        Ok(true)
    }

    async fn attach(&self, id: ObjectId, key: &str) -> Option<Publishing> {
        let grant = self.shared.gate.authorize(id, key).await?;
        for _ in 0..3 {
            let live = self.shared.live_for(id, &grant.name).await;
            match live.attach(key, self.cancel.clone()).await {
                Ok(t) => {
                    return Some(Publishing {
                        live,
                        serial: t.serial,
                        stream_id: 0,
                        armed: true,
                    });
                }
                Err(Attach::Ended) => self.shared.remove_live(&live).await,
                Err(Attach::Refused) => return None,
            }
        }
        None
    }
}

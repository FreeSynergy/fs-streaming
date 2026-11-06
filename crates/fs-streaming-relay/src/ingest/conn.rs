//! The state of one sender connection and what it does with each message.
//!
//! Pattern: State machine over `rml_rtmp`'s `ServerSession`, which only ever sees commands and
//! control messages (re-chunked by us). Media goes straight to the hub, and only media on the
//! published stream id, after `publish`, is forwarded. A wrong key and an unknown stream get the
//! same refusal.

use std::sync::Arc;
use std::time::{Duration, Instant};

use fs_streaming_core::ObjectId;
use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::messages::MessagePayload;
use rml_rtmp::sessions::{ServerSession, ServerSessionEvent, ServerSessionResult};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;

use super::acks::Acks;
use super::path::parse;
use super::publishing::Publishing;
use crate::error::WireError;
use crate::frame::{Frame, Kind};
use crate::live::Attach;
use crate::relay::Shared;
use crate::task::CancelToken;

pub struct Conn<'a> {
    pub shared: &'a Arc<Shared>,
    pub io: &'a mut TcpStream,
    pub cancel: CancelToken,
    pub session: ServerSession,
    pub ser: ChunkSerializer,
    pub publishing: Option<Publishing>,
    /// The connection's place among the limited ones, held until it has published.
    pub permit: Option<OwnedSemaphorePermit>,
    /// When an answer must have been written, if it has not published yet.
    pub deadline: Instant,
    /// Bytes read from the socket, the window the sender announced, and what was acknowledged.
    pub acks: Acks,
}

pub async fn write(io: &mut TcpStream, bytes: &[u8], stall: Duration) -> Result<(), WireError> {
    tokio::time::timeout(stall, io.write_all(bytes))
        .await
        .map_err(|_| WireError::Timeout)??;
    Ok(())
}

impl Conn<'_> {
    pub async fn respond(
        &mut self,
        results: Vec<ServerSessionResult>,
    ) -> Result<Vec<ServerSessionEvent>, WireError> {
        let mut events = Vec::new();
        for r in results {
            match r {
                ServerSessionResult::OutboundResponse(p) => {
                    let stall = self.write_limit();
                    write(self.io, &p.bytes, stall).await?;
                }
                ServerSessionResult::RaisedEvent(e) => events.push(e),
                ServerSessionResult::UnhandleableMessageReceived(_) => {}
            }
        }
        Ok(events)
    }

    /// One message, in order. `Ok(false)` ends the connection cleanly.
    pub async fn message(&mut self, m: MessagePayload) -> Result<bool, WireError> {
        match m.type_id {
            2..=6 | 17 | 20 => {
                if m.type_id == 5 && m.data.len() >= 4 {
                    let w = [m.data[0], m.data[1], m.data[2], m.data[3]];
                    self.acks.window = u64::from(u32::from_be_bytes(w));
                }
                // A command rml_amf0 cannot read (a Date, say) is skipped, not a reason to disconnect.
                let readable = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    m.to_rtmp_message().is_ok()
                }));
                if !readable.unwrap_or(false) {
                    return Ok(true);
                }
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
                            .publish(p.serial, Frame::new(kind, m.timestamp.value, m.data));
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
                self.permit = None;
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

//! The connection side of the test RTMP server.

use std::sync::{Arc, Mutex};

use rml_rtmp::chunk_io::ChunkDeserializer;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;

use super::sink::{Got, Mode};

async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S) -> Vec<u8> {
    let mut hs = Handshake::new(PeerType::Server);
    let mut buf = [0u8; 4096];
    loop {
        let n = s.read(&mut buf).await.unwrap_or(0);
        if n == 0 {
            return vec![];
        }
        match hs.process_bytes(&buf[..n]) {
            Ok(HandshakeProcessResult::InProgress { response_bytes }) => {
                s.write_all(&response_bytes).await.unwrap()
            }
            Ok(HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            }) => {
                s.write_all(&response_bytes).await.unwrap();
                return remaining_bytes;
            }
            Err(_) => return vec![],
        }
    }
}

pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut s: S,
    mode: Mode,
    got: Arc<Mutex<Vec<Got>>>,
    pubs: Arc<Mutex<Vec<(String, String)>>>,
    mut rx: watch::Receiver<bool>,
) {
    if mode == Mode::Silent {
        let mut b = [0u8; 1024];
        while matches!(s.read(&mut b).await, Ok(n) if n > 0) {}
        return;
    }
    let mut early = handshake(&mut s).await;
    let (mut session, first) = ServerSession::new(ServerSessionConfig::new()).unwrap();
    let mut de = ChunkDeserializer::new();
    let mut pending = first;
    let mut buf = vec![0u8; 16 * 1024];
    let mut stalled = false;
    loop {
        while !pending.is_empty() {
            let batch = std::mem::take(&mut pending);
            for r in batch {
                match r {
                    ServerSessionResult::OutboundResponse(p) => {
                        if s.write_all(&p.bytes).await.is_err() {
                            return;
                        }
                    }
                    ServerSessionResult::RaisedEvent(ServerSessionEvent::ConnectionRequested {
                        request_id,
                        ..
                    }) => {
                        pending.extend(session.accept_request(request_id).unwrap());
                    }
                    ServerSessionResult::RaisedEvent(
                        ServerSessionEvent::PublishStreamRequested {
                            request_id,
                            app_name,
                            stream_key,
                            ..
                        },
                    ) => {
                        pubs.lock().unwrap().push((app_name, stream_key.clone()));
                        if mode == Mode::Refuse {
                            pending.extend(
                                session
                                    .reject_request(
                                        request_id,
                                        "NetStream.Publish.BadName",
                                        &format!("refused {stream_key}"),
                                    )
                                    .unwrap(),
                            );
                        } else {
                            pending.extend(session.accept_request(request_id).unwrap());
                            stalled = mode == Mode::Stall;
                        }
                    }
                    _ => {}
                }
            }
        }
        if stalled {
            let _ = rx.wait_for(|v| *v).await;
            stalled = false;
        }
        let n = if early.is_empty() {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            }
        } else {
            let e = std::mem::take(&mut early);
            buf[..e.len()].copy_from_slice(&e);
            e.len()
        };
        let mut next: &[u8] = &buf[..n];
        while let Ok(Some(m)) = de.get_next_message(next) {
            next = &[];
            if m.type_id == 1 {
                de.set_max_chunk_size(u32::from_be_bytes([
                    m.data[0], m.data[1], m.data[2], m.data[3],
                ]) as usize)
                    .unwrap();
            }
            if matches!(m.type_id, 8 | 9 | 18) {
                got.lock().unwrap().push(Got {
                    type_id: m.type_id,
                    ts: m.timestamp.value,
                    data: m.data,
                });
            }
        }
        match session.handle_input(&buf[..n]) {
            Ok(r) => pending = r,
            Err(_) => return,
        }
    }
}

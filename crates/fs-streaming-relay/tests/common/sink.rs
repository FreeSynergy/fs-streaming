//! A local RTMP server for tests: takes what targets send and writes down every message.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use rml_rtmp::chunk_io::ChunkDeserializer;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

/// How the server behaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Accept every connect and publish.
    Accept,
    /// Accept the connect, refuse the publish explicitly.
    Refuse,
    /// Accept TCP and say nothing at all.
    Silent,
    /// Accept, then stop reading until released.
    Stall,
    /// Accept TCP and hang up at once.
    HangUp,
}

/// One message received.
#[derive(Clone, Debug)]
pub struct Got {
    pub type_id: u8,
    pub ts: u32,
    pub data: Bytes,
}

pub struct Sink {
    pub addr: SocketAddr,
    pub got: Arc<Mutex<Vec<Got>>>,
    pub accepts: Arc<Mutex<Vec<Instant>>>,
    pub publishes: Arc<Mutex<Vec<(String, String)>>>,
    release: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Sink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Sink {
    pub async fn start(mode: Mode) -> Sink {
        Sink::start_with(mode, None).await
    }

    /// With TLS in front when an acceptor is given.
    pub async fn start_with(mode: Mode, tls: Option<tokio_rustls::TlsAcceptor>) -> Sink {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        if mode == Mode::Stall {
            socket.set_recv_buffer_size(4096).unwrap();
        }
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener: TcpListener = socket.listen(64).unwrap();
        let addr = listener.local_addr().unwrap();
        let (got, accepts, publishes) = (
            Arc::new(Mutex::new(vec![])),
            Arc::new(Mutex::new(vec![])),
            Arc::new(Mutex::new(vec![])),
        );
        let (release, rx) = watch::channel(false);
        let (g, a, p) = (got.clone(), accepts.clone(), publishes.clone());
        let task = tokio::spawn(async move {
            let mut conns = tokio::task::JoinSet::new();
            loop {
                let Ok((s, _)) = listener.accept().await else {
                    return;
                };
                a.lock().unwrap().push(Instant::now());
                if mode != Mode::HangUp {
                    let (g, p, rx, tls) = (g.clone(), p.clone(), rx.clone(), tls.clone());
                    conns.spawn(async move {
                        match tls {
                            Some(t) => {
                                if let Ok(secure) = t.accept(s).await {
                                    serve(secure, mode, g, p, rx).await;
                                }
                            }
                            None => serve(s, mode, g, p, rx).await,
                        }
                    });
                }
            }
        });
        Sink {
            addr,
            got,
            accepts,
            publishes,
            release,
            task,
        }
    }

    /// `rtmp://127.0.0.1:port/app`.
    pub fn url(&self, app: &str) -> String {
        format!("rtmp://{}/{app}", self.addr)
    }

    pub fn release(&self) {
        self.release.send_replace(true);
    }

    pub fn messages(&self) -> Vec<Got> {
        self.got.lock().unwrap().clone()
    }

    pub fn media_count(&self) -> usize {
        self.got
            .lock()
            .unwrap()
            .iter()
            .filter(|g| g.type_id == 8 || g.type_id == 9)
            .count()
    }
}

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

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
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
                        pubs.lock().unwrap().push((app_name, stream_key));
                        if mode == Mode::Refuse {
                            pending.extend(
                                session
                                    .reject_request(
                                        request_id,
                                        "NetStream.Publish.BadName",
                                        "secret-from-the-far-end",
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

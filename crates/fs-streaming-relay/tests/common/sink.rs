//! A local RTMP server for tests: takes what targets send and writes down every message.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::watch;

use super::sink_serve as serve;

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
    /// Accept the publish, then close the connection.
    DropAfterPublish,
    /// Accept the publish, and say no when the first media arrives.
    RefuseLater,
    /// Send a harmless `onStatus` before answering `connect`.
    Chatty,
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
    #[allow(dead_code)] // never read: dropping it ends the runtime thread of `start_threaded`
    stop_on_drop: Option<std::sync::mpsc::Sender<()>>,
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
    #[allow(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        clippy::many_single_char_names
    )] // async keeps the call sites uniform; g/a/p are the shared logs
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
                                    serve::serve(secure, mode, g, p, rx).await;
                                }
                            }
                            None => serve::serve(s, mode, g, p, rx).await,
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
            stop_on_drop: None,
        }
    }

    /// A sink on a runtime of its own, so counting the tasks of the test's runtime is not disturbed by it.
    pub fn isolated(mode: Mode) -> Sink {
        let (tx, rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            tx.send(rt.block_on(Sink::start(mode))).unwrap();
            let _ = stop_rx.recv();
        });
        let mut sink = rx.recv().unwrap();
        sink.stop_on_drop = Some(stop_tx);
        sink
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

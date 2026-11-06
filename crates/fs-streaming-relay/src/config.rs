//! Settings of the relay.
//!
//! Pattern: Builder — `RelayConfig::new()` has safe defaults; `with_*` methods change one thing. The
//! hooks that only tests may use are behind the `test-hooks` feature. There is no way to switch off
//! certificate checking.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fs_streaming_core::RecordingSink;

/// Limits against slow, silent or hostile peers.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Largest chunk size a peer may announce.
    pub max_chunk: u32,
    /// Largest message accepted.
    pub max_message: usize,
    /// Time allowed for the handshake.
    pub handshake_timeout: Duration,
    /// A sender that sends nothing for this long is treated as dropped.
    pub idle_timeout: Duration,
    /// A write that makes no progress for this long ends the connection to a target.
    pub write_stall: Duration,
    /// Time allowed for TCP and TLS.
    pub connect_timeout: Duration,
    /// Time allowed for an answer to a command.
    pub answer_timeout: Duration,
    /// Largest command message (AMF) accepted; nested values are parsed recursively.
    pub max_amf: usize,
    /// Connections accepted at once.
    pub max_connections: usize,
    /// Time from connecting to a accepted `publish`.
    pub publish_deadline: Duration,
    /// Time a closing recording may take before it is cut off.
    pub recorder_finish: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_chunk: 16 * 1024 * 1024,
            max_message: 16 * 1024 * 1024,
            handshake_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            write_stall: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            answer_timeout: Duration::from_secs(10),
            max_amf: 8 * 1024,
            max_connections: 64,
            publish_deadline: Duration::from_secs(10),
            recorder_finish: Duration::from_secs(10),
        }
    }
}

/// Everything a host can set.
#[derive(Clone)]
pub struct RelayConfig {
    pub(crate) bind: SocketAddr,
    pub(crate) limits: Limits,
    pub(crate) grace: Duration,
    pub(crate) retry_min: Duration,
    pub(crate) retry_max: Duration,
    pub(crate) queue_bytes: usize,
    pub(crate) record_queue_bytes: usize,
    pub(crate) sink: Option<Arc<dyn RecordingSink>>,
    pub(crate) extra_roots: Vec<Vec<u8>>,
    pub(crate) advertise: Option<String>,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            limits: Limits::default(),
            grace: fs_streaming_core::INTERRUPT_GRACE,
            retry_min: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            queue_bytes: 8 * 1024 * 1024,
            record_queue_bytes: 64 * 1024 * 1024,
            sink: None,
            extra_roots: Vec::new(),
            advertise: None,
        }
    }
}

impl RelayConfig {
    /// Defaults: loopback, an OS-chosen port, no recording sink.
    pub fn new() -> Self {
        Self::default()
    }
    /// Where to listen. The default is `127.0.0.1` on a port the OS picks; ingest is plain RTMP, so a
    /// host that listens beyond the machine needs its own TLS in front.
    #[must_use]
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.bind = addr;
        self
    }
    /// The host name or address senders are told to use in the ingest address. Needed when the bind
    /// address is a wildcard; without it a wildcard bind advertises `localhost`.
    #[must_use]
    pub fn advertise(mut self, host: impl Into<String>) -> Self {
        self.advertise = Some(host.into());
        self
    }
    /// Limits against slow, silent or hostile peers.
    #[must_use]
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    /// Pauses between tries to reach a lost target: grow from `min` to `max`, with jitter.
    #[must_use]
    pub fn retry(mut self, min: Duration, max: Duration) -> Self {
        self.retry_min = min;
        self.retry_max = max.max(min);
        self
    }
    /// Bytes queued per target before frames are dropped up to the next keyframe.
    #[must_use]
    pub fn queue_bytes(mut self, bytes: usize) -> Self {
        self.queue_bytes = bytes.max(1);
        self
    }
    /// Where recordings go.
    #[must_use]
    pub fn sink(mut self, sink: Arc<dyn RecordingSink>) -> Self {
        self.sink = Some(sink);
        self
    }
    /// Test only: a shorter time for `interrupted`.
    #[cfg(feature = "test-hooks")]
    #[must_use]
    pub fn grace_for_tests(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }
    /// Test only: trust one more root certificate (DER), next to the built-in roots.
    #[cfg(feature = "test-hooks")]
    #[must_use]
    pub fn trust_root_for_tests(mut self, der: Vec<u8>) -> Self {
        self.extra_roots.push(der);
        self
    }
}

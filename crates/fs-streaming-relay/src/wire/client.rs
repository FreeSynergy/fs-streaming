//! Our small RTMP publishing client.
//!
//! Pattern: Adapter over `rml_rtmp`'s AMF0 messages, on a [`Link`]. Media goes out exactly as it came
//! (`rml_rtmp`'s own client session re-writes metadata and hides the stream id, so it is not used).
//! Replies are read until the one we wait for; an explicit refusal is told apart from silence. The far
//! end's own words are never kept.

use std::collections::HashMap;

use rml_rtmp::handshake::PeerType;
use rml_rtmp::messages::{MessagePayload, RtmpMessage};
use rml_rtmp::rml_amf0::Amf0Value;
use rml_rtmp::time::RtmpTimestamp;

use super::link::Link;
use super::reply::{Reply, parse};
use super::{Duplex, handshake};
use crate::config::Limits;
use crate::error::WireError;
use crate::frame::Frame;

/// What the far end said to `publish`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// `NetStream.Publish.Start`.
    Accepted,
    /// An explicit no.
    Refused,
}

/// One RTMP connection as the publishing side.
pub struct Client {
    link: Link,
    stream_id: u32,
    limits: Limits,
}

impl Client {
    /// Handshake, then announce a larger chunk size.
    pub async fn open(mut io: Box<dyn Duplex>, limits: Limits) -> Result<Self, WireError> {
        let left = handshake(&mut io, PeerType::Client, limits.handshake_timeout).await?;
        Ok(Self {
            link: Link::new(io, limits, &left)?,
            stream_id: 0,
            limits,
        })
    }

    /// `connect` to an application. `Ok(false)` is an explicit refusal.
    pub async fn connect(&mut self, app: &str, tc_url: &str) -> Result<bool, WireError> {
        let text = |s: &str| Amf0Value::Utf8String(s.into());
        let mut o = HashMap::new();
        o.insert("app".to_string(), text(app));
        o.insert("type".to_string(), text("nonprivate"));
        o.insert(
            "flashVer".to_string(),
            text("FMLE/3.0 (compatible; FreeSynergy)"),
        );
        o.insert("tcUrl".to_string(), text(tc_url));
        self.command("connect", 1.0, Amf0Value::Object(o), vec![], 0)
            .await?;
        Ok(matches!(self.reply(true).await?, Reply::Result(_)))
    }

    /// `createStream` and `publish`. Sends no media.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // checked to be 1..1e9
    pub async fn publish(&mut self, key: &str) -> Result<PublishOutcome, WireError> {
        self.command("createStream", 2.0, Amf0Value::Null, vec![], 0)
            .await?;
        match self.reply(true).await? {
            Reply::Result(args) => match args.into_iter().find_map(Amf0Value::get_number) {
                Some(n) if (1.0..1e9).contains(&n) => self.stream_id = n as u32,
                _ => return Err(WireError::Protocol),
            },
            _ => return Ok(PublishOutcome::Refused),
        }
        let args = vec![
            Amf0Value::Utf8String(key.into()),
            Amf0Value::Utf8String("live".into()),
        ];
        self.command("publish", 0.0, Amf0Value::Null, args, self.stream_id)
            .await?;
        loop {
            match self.reply(false).await? {
                Reply::Status { code, .. } if code == "NetStream.Publish.Start" => {
                    return Ok(PublishOutcome::Accepted);
                }
                Reply::Status { error: true, .. } | Reply::Error => {
                    return Ok(PublishOutcome::Refused);
                }
                _ => {}
            }
        }
    }

    /// Say goodbye politely. Errors do not matter.
    pub async fn close(&mut self) {
        let id = Amf0Value::Number(f64::from(self.stream_id));
        let _ = self
            .command("deleteStream", 0.0, Amf0Value::Null, vec![id], 0)
            .await;
        self.link.shutdown().await;
    }

    /// Send one frame, untouched, with its timestamp. Returns the bytes written.
    pub async fn send_frame(&mut self, f: &Frame) -> Result<u64, WireError> {
        let p = MessagePayload {
            timestamp: RtmpTimestamp::new(f.timestamp),
            type_id: f.kind.type_id(),
            message_stream_id: self.stream_id,
            data: f.payload.clone(),
        };
        let n = self.link.queue(&p)?;
        self.link.flush().await?;
        Ok(n)
    }

    /// Wait for bytes and read them. Cancel-safe. Look at the result with [`Client::take_error`].
    pub async fn read_step(&mut self) -> Result<(), WireError> {
        self.link.read_step().await
    }

    /// Write what is waiting (acks, pongs).
    pub async fn flush(&mut self) -> Result<(), WireError> {
        self.link.flush().await
    }

    /// Whether messages read so far held an `onStatus` error or an `_error`.
    pub fn take_error(&mut self) -> bool {
        let mut bad = false;
        for m in std::mem::take(&mut self.link.pending) {
            bad |= matches!(
                parse(&m),
                Some(Reply::Error | Reply::Status { error: true, .. })
            );
        }
        bad
    }

    async fn command(
        &mut self,
        name: &str,
        tx: f64,
        obj: Amf0Value,
        args: Vec<Amf0Value>,
        stream: u32,
    ) -> Result<(), WireError> {
        let msg = RtmpMessage::Amf0Command {
            command_name: name.into(),
            transaction_id: tx,
            command_object: obj,
            additional_arguments: args,
        };
        let p = msg
            .into_message_payload(RtmpTimestamp::new(0), stream)
            .map_err(|_| WireError::Protocol)?;
        self.link.queue(&p)?;
        self.link.flush().await
    }

    /// The next reply that matters; `results` says whether `_result` counts.
    async fn reply(&mut self, results: bool) -> Result<Reply, WireError> {
        let wait = self.limits.answer_timeout;
        tokio::time::timeout(wait, self.reply_inner(results))
            .await
            .map_err(|_| WireError::Timeout)?
    }

    async fn reply_inner(&mut self, results: bool) -> Result<Reply, WireError> {
        loop {
            while let Some(m) = self.link.pending.pop_front() {
                if let Some(r) = parse(&m).filter(|r| results || !matches!(r, Reply::Result(_))) {
                    return Ok(r);
                }
            }
            self.link.read_step().await?;
            self.link.flush().await?;
        }
    }
}

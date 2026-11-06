//! More ways to misbehave for the raw publisher: batches, other stream ids, split steps.

use std::time::Duration;

use bytes::Bytes;
use rml_rtmp::messages::{MessagePayload, RtmpMessage};
use rml_rtmp::rml_amf0::Amf0Value;
use rml_rtmp::time::RtmpTimestamp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::raw::Raw;

pub fn payload(type_id: u8, ts: u32, stream: u32, data: &[u8]) -> MessagePayload {
    MessagePayload {
        timestamp: RtmpTimestamp::new(ts),
        type_id,
        message_stream_id: stream,
        data: Bytes::copy_from_slice(data),
    }
}

/// A command as a message, to be sent with others in one write.
pub fn command(name: &str, args: Vec<Amf0Value>, stream: u32) -> MessagePayload {
    let m = RtmpMessage::Amf0Command {
        command_name: name.into(),
        transaction_id: 0.0,
        command_object: Amf0Value::Null,
        additional_arguments: args,
    };
    m.into_message_payload(RtmpTimestamp::new(0), stream)
        .unwrap()
}

impl Raw {
    /// Everything up to and including `createStream`; the stream id is kept.
    pub async fn prepare(&mut self, app: &str) {
        let mut o = std::collections::HashMap::new();
        o.insert("app".to_string(), Amf0Value::Utf8String(app.into()));
        self.command("connect", 1.0, Amf0Value::Object(o), vec![], 0)
            .await;
        while let Some((cmd, _, _)) = self.next_command().await {
            if cmd == "_result" {
                break;
            }
        }
        self.command("createStream", 2.0, Amf0Value::Null, vec![], 0)
            .await;
        while let Some((cmd, _, args)) = self.next_command().await {
            if cmd == "_result" {
                self.stream_id = args
                    .iter()
                    .find_map(|a| {
                        if let Amf0Value::Number(n) = a {
                            Some(*n as u32)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(1);
                break;
            }
        }
    }

    /// The `publish` command as a message.
    pub fn publish_message(&self, name: &str) -> MessagePayload {
        command(
            "publish",
            vec![
                Amf0Value::Utf8String(name.into()),
                Amf0Value::Utf8String("live".into()),
            ],
            self.stream_id,
        )
    }

    /// Serialise several messages and write them in one go.
    pub async fn batch(&mut self, msgs: &[MessagePayload]) {
        let mut all = Vec::new();
        for m in msgs {
            all.extend(self.ser.serialize(m, false, false).unwrap().bytes);
        }
        self.io.write_all(&all).await.unwrap();
    }

    /// Wait for the answer to `publish`.
    pub async fn answer(&mut self) -> bool {
        while let Some((cmd, code, _)) = self.next_command().await {
            if cmd == "onStatus" {
                return code == "NetStream.Publish.Start";
            }
        }
        false
    }
}

/// The connection was closed by the relay.
pub async fn is_closed(raw: &mut Raw) -> bool {
    let mut b = [0u8; 4096];
    loop {
        match tokio::time::timeout(Duration::from_secs(10), raw.io.read(&mut b)).await {
            Ok(Ok(0) | Err(_)) => return true,
            Ok(Ok(_)) => {}
            Err(_) => return false,
        }
    }
}

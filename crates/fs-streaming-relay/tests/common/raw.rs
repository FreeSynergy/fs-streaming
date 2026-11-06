//! A raw RTMP publisher for tests: sends exactly the messages it is told to.

use bytes::Bytes;
use rml_rtmp::chunk_io::{ChunkDeserializer, ChunkSerializer};
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::messages::{MessagePayload, RtmpMessage};
use rml_rtmp::rml_amf0::Amf0Value;
use rml_rtmp::time::RtmpTimestamp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A connection that can send anything.
pub struct Raw {
    pub io: TcpStream,
    de: ChunkDeserializer,
    ser: ChunkSerializer,
    stream_id: u32,
}

/// What the far end answered to `publish`.
#[derive(Debug, PartialEq, Eq)]
pub enum Answer {
    Accepted,
    Refused(String),
}

impl Raw {
    pub async fn connect(addr: std::net::SocketAddr) -> Raw {
        let mut io = TcpStream::connect(addr).await.unwrap();
        let mut hs = Handshake::new(PeerType::Client);
        io.write_all(&hs.generate_outbound_p0_and_p1().unwrap())
            .await
            .unwrap();
        let mut buf = [0u8; 4096];
        loop {
            let n = io.read(&mut buf).await.unwrap();
            assert!(n > 0, "closed in the handshake");
            match hs.process_bytes(&buf[..n]).unwrap() {
                HandshakeProcessResult::InProgress { response_bytes } => {
                    io.write_all(&response_bytes).await.unwrap()
                }
                HandshakeProcessResult::Completed { response_bytes, .. } => {
                    io.write_all(&response_bytes).await.unwrap();
                    break;
                }
            }
        }
        Raw {
            io,
            de: ChunkDeserializer::new(),
            ser: ChunkSerializer::new(),
            stream_id: 0,
        }
    }

    async fn command(
        &mut self,
        name: &str,
        tx: f64,
        obj: Amf0Value,
        args: Vec<Amf0Value>,
        stream: u32,
    ) {
        let m = RtmpMessage::Amf0Command {
            command_name: name.into(),
            transaction_id: tx,
            command_object: obj,
            additional_arguments: args,
        };
        let p = m
            .into_message_payload(RtmpTimestamp::new(0), stream)
            .unwrap();
        self.write_payload(&p).await;
    }

    pub async fn write_payload(&mut self, p: &MessagePayload) {
        let pkt = self.ser.serialize(p, false, false).unwrap();
        self.io.write_all(&pkt.bytes).await.unwrap();
    }

    /// The next command from the far end, as (name, status code or empty).
    async fn next_command(&mut self) -> Option<(String, String, Vec<Amf0Value>)> {
        let mut buf = [0u8; 4096];
        let mut feed: Vec<u8> = Vec::new();
        loop {
            while let Some(p) = self
                .de
                .get_next_message(&std::mem::take(&mut feed))
                .unwrap()
            {
                if p.type_id == 1 {
                    let n = u32::from_be_bytes([p.data[0], p.data[1], p.data[2], p.data[3]]);
                    self.de.set_max_chunk_size(n as usize).unwrap();
                }
                if let Ok(RtmpMessage::Amf0Command {
                    command_name,
                    additional_arguments,
                    ..
                }) = p.to_rtmp_message()
                {
                    let code = additional_arguments.iter().find_map(|a| match a {
                        Amf0Value::Object(o) => match o.get("code") {
                            Some(Amf0Value::Utf8String(c)) => Some(c.clone()),
                            _ => None,
                        },
                        _ => None,
                    });
                    return Some((command_name, code.unwrap_or_default(), additional_arguments));
                }
            }
            let n = self.io.read(&mut buf).await.ok()?;
            if n == 0 {
                return None;
            }
            feed.extend_from_slice(&buf[..n]);
        }
    }

    /// connect, createStream, publish; `app` and `name` are sent as given.
    pub async fn publish(&mut self, app: &str, name: &str) -> Answer {
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
        self.command(
            "publish",
            0.0,
            Amf0Value::Null,
            vec![
                Amf0Value::Utf8String(name.into()),
                Amf0Value::Utf8String("live".into()),
            ],
            self.stream_id,
        )
        .await;
        while let Some((cmd, code, _)) = self.next_command().await {
            if cmd == "onStatus" && code == "NetStream.Publish.Start" {
                return Answer::Accepted;
            }
            if cmd == "onStatus" || cmd == "_error" {
                return Answer::Refused(code);
            }
        }
        Answer::Refused("closed".into())
    }

    /// Announce a chunk size and use it.
    pub async fn set_chunk_size(&mut self, n: u32) {
        let p = self
            .ser
            .set_max_chunk_size(n, RtmpTimestamp::new(0))
            .unwrap();
        self.io.write_all(&p.bytes).await.unwrap();
    }

    /// Send one message of any type on the publishing stream.
    pub async fn send(&mut self, type_id: u8, ts: u32, data: &[u8]) {
        let p = MessagePayload {
            timestamp: RtmpTimestamp::new(ts),
            type_id,
            message_stream_id: self.stream_id,
            data: Bytes::copy_from_slice(data),
        };
        self.write_payload(&p).await;
    }

    pub async fn video(&mut self, ts: u32, data: &[u8]) {
        self.send(9, ts, data).await;
    }
    pub async fn audio(&mut self, ts: u32, data: &[u8]) {
        self.send(8, ts, data).await;
    }
}

impl Raw {
    /// Metadata, then a video and an audio sequence header, all at time 0.
    pub async fn headers(&mut self) {
        self.send(18, 0, &metadata()).await;
        self.video(0, &[0x17, 0, 0, 0, 0, 0x01, 0x64, 0x00, 0x1f])
            .await;
        self.audio(0, &[0xAF, 0x00, 0x12, 0x10]).await;
    }

    /// Tick `n`: a video frame of `size` bytes (a keyframe every ten) and an audio frame, both at `n * 40`.
    pub async fn tick(&mut self, n: u32, size: usize) {
        let mut v = vec![if n % 10 == 0 { 0x17 } else { 0x27 }, 1, 0, 0, 0];
        v.resize(size, n as u8);
        self.video(n * 40, &v).await;
        self.audio(n * 40, &[0xAF, 0x01, n as u8, 1, 2, 3]).await;
    }
}

/// `@setDataFrame` + `onMetaData` + a small AMF0 object, as a sender would write it.
pub fn metadata() -> Vec<u8> {
    let mut m = vec![0x02, 0x00, 0x0d];
    m.extend_from_slice(b"@setDataFrame");
    m.extend_from_slice(&[0x02, 0x00, 0x0a]);
    m.extend_from_slice(b"onMetaData");
    m.extend_from_slice(&[0x03, 0x00, 0x05]);
    m.extend_from_slice(b"width");
    m.push(0x00);
    m.extend_from_slice(&320f64.to_be_bytes());
    m.extend_from_slice(&[0x00, 0x00, 0x09]);
    m
}

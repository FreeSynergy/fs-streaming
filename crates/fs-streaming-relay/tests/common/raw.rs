//! A raw RTMP publisher for tests: sends exactly the messages it is told to.

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
    pub de: ChunkDeserializer,
    pub ser: ChunkSerializer,
    pub stream_id: u32,
}

/// What the far end answered to `publish`.
#[derive(Debug, PartialEq, Eq)]
pub enum Answer {
    Accepted,
    Refused(String),
}

impl Raw {
    pub async fn connect(addr: std::net::SocketAddr) -> Raw {
        Raw::connect_with(addr, None).await
    }

    /// With a receive buffer of this size, so that a client that stops reading blocks the relay soon.
    pub async fn connect_with(addr: std::net::SocketAddr, rcvbuf: Option<u32>) -> Raw {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        if let Some(n) = rcvbuf {
            socket.set_recv_buffer_size(n).unwrap();
        }
        let mut io = socket.connect(addr).await.unwrap();
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

    pub async fn command(
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
    pub async fn next_command(&mut self) -> Option<(String, String, Vec<Amf0Value>)> {
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
}

#[allow(unused_imports)]
pub use super::frames::metadata;

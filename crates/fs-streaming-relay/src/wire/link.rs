//! `Link`: the plumbing of one RTMP connection as a client.
//!
//! Pattern: Adapter over `rml_rtmp`'s chunk serializer and our `MessageReader` — bytes in become
//! messages (`pending`), bytes out wait in an outbox so a read never has to write in the middle of a
//! select. Acknowledgements and ping answers are queued here, in order, with everything else.

use std::collections::VecDeque;

use bytes::Bytes;
use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::messages::{MessagePayload, RtmpMessage};
use rml_rtmp::time::RtmpTimestamp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{Duplex, MessageReader};
use crate::config::Limits;
use crate::error::WireError;

/// Reading, writing, acknowledging.
pub struct Link {
    io: Box<dyn Duplex>,
    reader: MessageReader,
    ser: ChunkSerializer,
    buf: Vec<u8>,
    outbox: Vec<u8>,
    bytes_in: u64,
    last_ack: u64,
    window: u64,
    /// Messages read and not yet looked at.
    pub pending: VecDeque<MessagePayload>,
}

impl Link {
    /// After the handshake; `left` are bytes that came with it.
    pub fn new(io: Box<dyn Duplex>, limits: Limits, left: &[u8]) -> Result<Self, WireError> {
        let mut l = Self {
            io,
            reader: MessageReader::new(limits),
            ser: ChunkSerializer::new(),
            buf: vec![0; 16 * 1024],
            outbox: Vec::new(),
            bytes_in: 0,
            last_ack: 0,
            window: 0,
            pending: VecDeque::new(),
        };
        if !left.is_empty() {
            l.take(left)?;
        }
        let set = l
            .ser
            .set_max_chunk_size(4096, RtmpTimestamp::new(0))
            .map_err(|_| WireError::Protocol)?;
        l.outbox.extend(set.bytes);
        Ok(l)
    }

    /// Wait for bytes and read them. Cancel-safe.
    pub async fn read_step(&mut self) -> Result<(), WireError> {
        let n = self.io.read(&mut self.buf).await?;
        if n == 0 {
            return Err(WireError::Closed);
        }
        let bytes = Bytes::copy_from_slice(&self.buf[..n]);
        self.take(&bytes)
    }

    /// Write what is waiting (acks, pongs, queued messages).
    pub async fn flush(&mut self) -> Result<(), WireError> {
        if !self.outbox.is_empty() {
            self.io.write_all(&self.outbox).await?;
            self.outbox.clear();
        }
        Ok(())
    }

    /// Say goodbye to the transport.
    pub async fn shutdown(&mut self) {
        let _ = self.io.shutdown().await;
    }

    /// Queue one message; returns the bytes it takes on the wire.
    pub fn queue(&mut self, p: &MessagePayload) -> Result<u64, WireError> {
        let pkt = self
            .ser
            .serialize(p, false, false)
            .map_err(|_| WireError::Protocol)?;
        let n = pkt.bytes.len() as u64;
        self.outbox.extend(pkt.bytes);
        Ok(n)
    }

    fn take(&mut self, bytes: &[u8]) -> Result<(), WireError> {
        self.bytes_in += bytes.len() as u64;
        for m in self.reader.feed(bytes)? {
            self.control(&m)?;
            self.pending.push_back(m);
        }
        if self.window > 0 && self.bytes_in - self.last_ack >= self.window {
            self.last_ack = self.bytes_in;
            let ack = RtmpMessage::Acknowledgement {
                sequence_number: (self.bytes_in & 0xffff_ffff) as u32,
            };
            let p = ack
                .into_message_payload(RtmpTimestamp::new(0), 0)
                .map_err(|_| WireError::Protocol)?;
            self.queue(&p)?;
        }
        Ok(())
    }

    fn control(&mut self, m: &MessagePayload) -> Result<(), WireError> {
        match (m.type_id, m.data.len()) {
            (5, 4..) => {
                let size = [m.data[0], m.data[1], m.data[2], m.data[3]];
                self.window = u64::from(u32::from_be_bytes(size));
            }
            (4, 6..) if m.data[0] == 0 && m.data[1] == 6 => {
                let mut pong = vec![0, 7];
                pong.extend_from_slice(&m.data[2..6]);
                let p = MessagePayload {
                    timestamp: RtmpTimestamp::new(0),
                    type_id: 4,
                    message_stream_id: 0,
                    data: Bytes::from(pong),
                };
                self.queue(&p)?;
            }
            _ => {}
        }
        Ok(())
    }
}

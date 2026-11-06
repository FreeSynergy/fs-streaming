//! `MessageReader`: bytes in, whole RTMP messages out, with limits.
//!
//! Pattern: Adapter over `rml_rtmp::chunk_io::ChunkDeserializer` — the payloads come out exactly as
//! sent. It follows `SetChunkSize` itself and refuses a chunk size or a message above the limits, so a
//! hostile peer cannot make us hold more than a limited amount.

use rml_rtmp::chunk_io::ChunkDeserializer;
use rml_rtmp::messages::MessagePayload;

use crate::config::Limits;
use crate::error::WireError;

/// Reads messages from raw bytes.
pub struct MessageReader {
    de: ChunkDeserializer,
    limits: Limits,
}

impl MessageReader {
    /// A reader with the given limits.
    pub fn new(limits: Limits) -> Self {
        Self {
            de: ChunkDeserializer::new(),
            limits,
        }
    }

    /// Feed bytes; get every message that is now complete.
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<MessagePayload>, WireError> {
        let mut out = Vec::new();
        let mut next: &[u8] = bytes;
        loop {
            let got = self
                .de
                .get_next_message(next)
                .map_err(|_| WireError::Protocol)?;
            next = &[];
            let Some(msg) = got else { break };
            if msg.data.len() > self.limits.max_message {
                return Err(WireError::Limit);
            }
            if msg.type_id == 1 && msg.data.len() >= 4 {
                let size = u32::from_be_bytes([msg.data[0], msg.data[1], msg.data[2], msg.data[3]])
                    & 0x7fff_ffff;
                if size > self.limits.max_chunk || size == 0 {
                    return Err(WireError::Limit);
                }
                self.de
                    .set_max_chunk_size(size as usize)
                    .map_err(|_| WireError::Limit)?;
            }
            out.push(msg);
        }
        Ok(out)
    }
}

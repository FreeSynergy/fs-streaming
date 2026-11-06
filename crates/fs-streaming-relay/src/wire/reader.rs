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
            // `rml_rtmp` can panic on hostile chunk streams: that ends this connection only.
            let de = &mut self.de;
            let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                de.get_next_message(next)
            }))
            .map_err(|_| WireError::Protocol)?
            .map_err(|_| WireError::Protocol)?;
            next = &[];
            let Some(msg) = got else { break };
            let amf = matches!(msg.type_id, 15 | 17 | 20);
            if msg.data.len() > self.limits.max_message
                || (amf && msg.data.len() > self.limits.max_amf)
            {
                return Err(WireError::Limit);
            }
            // Commands are parsed by recursion later: refuse deep nesting before that.
            let body: &[u8] = if msg.type_id == 17 {
                msg.data.get(1..).unwrap_or(&[])
            } else {
                &msg.data
            };
            if matches!(msg.type_id, 17 | 20) && !super::amf::is_shallow(body) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rml_rtmp::chunk_io::ChunkSerializer;
    use rml_rtmp::time::RtmpTimestamp;

    fn chunks(type_id: u8, data: Vec<u8>) -> Vec<u8> {
        let p = MessagePayload {
            timestamp: RtmpTimestamp::new(0),
            type_id,
            message_stream_id: 0,
            data: Bytes::from(data),
        };
        ChunkSerializer::new()
            .serialize(&p, true, false)
            .unwrap()
            .bytes
    }

    #[test]
    fn a_command_of_nested_arrays_is_refused_before_it_is_parsed_even_on_a_small_stack() {
        let limits = Limits::default();
        let nested: Vec<u8> = (0..limits.max_amf / 5)
            .flat_map(|_| [0x0a, 0, 0, 0, 1])
            .collect();
        let big: Vec<u8> = (0..20_000).flat_map(|_| [0x0a, 0, 0, 0, 1]).collect();
        let t = std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let a = MessageReader::new(limits)
                    .feed(&chunks(20, nested))
                    .is_err();
                let b = MessageReader::new(limits).feed(&chunks(20, big)).is_err();
                a && b
            });
        assert!(
            t.unwrap().join().unwrap(),
            "both were refused, and nothing overflowed"
        );
    }

    #[test]
    fn a_plain_command_and_big_media_pass() {
        let mut r = MessageReader::new(Limits::default());
        let mut cmd = vec![2, 0, 4];
        cmd.extend_from_slice(b"ping");
        assert_eq!(r.feed(&chunks(20, cmd)).unwrap().len(), 1);
        assert_eq!(r.feed(&chunks(9, vec![0x17; 100_000])).unwrap().len(), 1);
    }
}

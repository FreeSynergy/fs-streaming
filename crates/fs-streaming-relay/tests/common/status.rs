//! An `onStatus` message written by hand, for servers that misbehave in specific ways.

use std::collections::HashMap;

use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::messages::RtmpMessage;
use rml_rtmp::rml_amf0::Amf0Value;
use rml_rtmp::time::RtmpTimestamp;

/// The bytes of one `onStatus` with this level and code, on the given message stream.
pub fn status_packet(level: &str, code: &str, stream: u32) -> Vec<u8> {
    let mut info = HashMap::new();
    info.insert("level".to_string(), Amf0Value::Utf8String(level.into()));
    info.insert("code".to_string(), Amf0Value::Utf8String(code.into()));
    let msg = RtmpMessage::Amf0Command {
        command_name: "onStatus".into(),
        transaction_id: 0.0,
        command_object: Amf0Value::Null,
        additional_arguments: vec![Amf0Value::Object(info)],
    };
    let payload = msg
        .into_message_payload(RtmpTimestamp::new(0), stream)
        .unwrap();
    ChunkSerializer::new()
        .serialize(&payload, true, false)
        .unwrap()
        .bytes
}

//! `Frame`: one forwarded RTMP message, payload untouched.
//!
//! Pattern: Newtype-like value object — the payload is shared (`Bytes`) and never changed. Only the
//! first bytes are read to tell a keyframe and a codec header from the rest (frame-type bits; for
//! enhanced RTMP the same bits plus the packet type, per its open specification). Nothing is decoded.

use bytes::Bytes;

/// What kind of message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// RTMP type 8.
    Audio,
    /// RTMP type 9.
    Video,
    /// RTMP type 18 (AMF0 data: metadata and the like).
    Data,
}

impl Kind {
    /// The RTMP message type id.
    pub const fn type_id(self) -> u8 {
        match self {
            Self::Audio => 8,
            Self::Video => 9,
            Self::Data => 18,
        }
    }

    /// From an RTMP type id, if it is one we forward.
    pub const fn from_type_id(id: u8) -> Option<Self> {
        match id {
            8 => Some(Self::Audio),
            9 => Some(Self::Video),
            18 => Some(Self::Data),
            _ => None,
        }
    }
}

/// One message with its timestamp and what the first bytes say about it.
#[derive(Clone, Debug)]
pub struct Frame {
    /// Kind.
    pub kind: Kind,
    /// RTMP timestamp, as sent.
    pub timestamp: u32,
    /// The payload, as sent.
    pub payload: Bytes,
    /// A video keyframe (not a codec header).
    pub keyframe: bool,
    /// A codec sequence header, or the metadata message.
    pub header: bool,
}

fn amf_string_at_start(p: &[u8], text: &str) -> bool {
    p.len() >= 3 + text.len()
        && p[0] == 2
        && usize::from(u16::from_be_bytes([p[1], p[2]])) == text.len()
        && &p[3..3 + text.len()] == text.as_bytes()
}

fn video_flags(p: &[u8]) -> (bool, bool) {
    let Some(&b0) = p.first() else {
        return (false, false);
    };
    if b0 & 0x80 != 0 {
        let header = matches!(b0 & 0x0f, 0);
        return (((b0 >> 4) & 7) == 1 && !header, header);
    }
    let header = matches!(b0 & 0x0f, 7 | 12) && p.get(1) == Some(&0);
    ((b0 >> 4) == 1 && !header, header)
}

fn audio_header(p: &[u8]) -> bool {
    match p.first() {
        Some(&b0) if b0 >> 4 == 10 => p.get(1) == Some(&0),
        Some(&b0) if b0 >> 4 == 9 => matches!(b0 & 0x0f, 0),
        _ => false,
    }
}

impl Frame {
    /// Classify a message by its first bytes.
    pub fn new(kind: Kind, timestamp: u32, payload: Bytes) -> Self {
        let (keyframe, header) = match kind {
            Kind::Video => video_flags(&payload),
            Kind::Audio => (false, audio_header(&payload)),
            Kind::Data => (
                false,
                amf_string_at_start(&payload, "@setDataFrame")
                    || amf_string_at_start(&payload, "onMetaData"),
            ),
        };
        Self {
            kind,
            timestamp,
            payload,
            keyframe,
            header,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(kind: Kind, bytes: &[u8]) -> Frame {
        Frame::new(kind, 0, Bytes::copy_from_slice(bytes))
    }

    #[test]
    fn legacy_avc_keyframe_and_header() {
        assert!(f(Kind::Video, &[0x17, 1, 0, 0, 0]).keyframe);
        assert!(!f(Kind::Video, &[0x27, 1, 0, 0, 0]).keyframe);
        let h = f(Kind::Video, &[0x17, 0, 0, 0, 0]);
        assert!(h.header && !h.keyframe);
    }

    #[test]
    fn enhanced_keyframe_av01() {
        // IsExHeader | FrameType 1 | PacketType 1 (coded frames), then FourCC "av01".
        let k = f(Kind::Video, &[0x91, b'a', b'v', b'0', b'1', 9]);
        assert!(k.keyframe && !k.header);
        let h = f(Kind::Video, &[0x90, b'a', b'v', b'0', b'1', 9]);
        assert!(h.header && !h.keyframe);
        assert!(!f(Kind::Video, &[0xA1, b'a', b'v', b'0', b'1']).keyframe);
    }

    #[test]
    fn audio_and_data_headers() {
        assert!(f(Kind::Audio, &[0xAF, 0, 0x12, 0x10]).header);
        assert!(!f(Kind::Audio, &[0xAF, 1, 0x21]).header);
        let mut meta = vec![2, 0, 13];
        meta.extend_from_slice(b"@setDataFrame");
        assert!(f(Kind::Data, &meta).header);
        assert!(!f(Kind::Data, &[2, 0, 3, b'a', b'b', b'c']).header);
    }
}

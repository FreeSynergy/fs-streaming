//! A small FLV writer: header, tags, previous-tag sizes. No decoding.
//!
//! Pattern: Strategy (format) — FLV holds what RTMP carried, enhanced RTMP included. The only change
//! is that the `@setDataFrame` prefix of a metadata message is left off, since FLV script data starts
//! with `onMetaData`.

use crate::frame::{Frame, Kind};

/// The FLV file header (audio and video flags set) and `PreviousTagSize0`.
pub const HEADER: [u8; 13] = [b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];

/// `\x02 \x00\x0d "@setDataFrame"`: 16 bytes.
const SET_DATA_FRAME: &[u8] = b"\x02\x00\x0d@setDataFrame";

/// One tag with its trailing previous-tag size.
#[allow(clippy::cast_possible_truncation)] // an RTMP message is below 2^24 bytes
pub fn tag(frame: &Frame) -> Vec<u8> {
    let mut data: &[u8] = &frame.payload;
    if frame.kind == Kind::Data && data.starts_with(SET_DATA_FRAME) {
        data = &data[SET_DATA_FRAME.len()..];
    }
    let len = data.len();
    let ts = frame.timestamp.to_be_bytes();
    let mut out = Vec::with_capacity(len + 15);
    out.push(frame.kind.type_id());
    out.extend_from_slice(&(len as u32).to_be_bytes()[1..]);
    out.extend_from_slice(&ts[1..]);
    out.push(ts[0]);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(data);
    out.extend_from_slice(&((len + 11) as u32).to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn header_bytes() {
        assert_eq!(
            HEADER,
            [
                0x46, 0x4C, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00
            ]
        );
    }

    #[test]
    fn video_tag_with_a_timestamp_beyond_24_bits() {
        let f = Frame::new(
            Kind::Video,
            0x0102_0304,
            Bytes::from_static(&[0x17, 0x01, 0xAA]),
        );
        assert_eq!(
            tag(&f),
            [
                0x09, 0, 0, 3, 0x02, 0x03, 0x04, 0x01, 0, 0, 0, 0x17, 0x01, 0xAA, 0, 0, 0, 14
            ]
        );
    }

    #[test]
    fn audio_tag_at_zero() {
        let f = Frame::new(Kind::Audio, 0, Bytes::from_static(&[0xAF, 0x00]));
        assert_eq!(
            tag(&f),
            [0x08, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0xAF, 0x00, 0, 0, 0, 13]
        );
    }

    #[test]
    fn metadata_loses_only_the_set_data_frame_prefix() {
        let mut p = SET_DATA_FRAME.to_vec();
        p.extend_from_slice(&[0x02, 0x00, 0x01, b'x']);
        let f = Frame::new(Kind::Data, 7, Bytes::from(p));
        assert_eq!(
            tag(&f),
            [
                0x12, 0, 0, 4, 0, 0, 7, 0, 0, 0, 0, 0x02, 0x00, 0x01, b'x', 0, 0, 0, 15
            ]
        );
    }
}

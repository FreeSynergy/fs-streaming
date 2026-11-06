//! Frames a test sender makes up: metadata, headers, ticks of video and audio.

use super::raw::Raw;

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

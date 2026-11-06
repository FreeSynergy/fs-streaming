//! `ffmpeg` and `ffprobe`: test tools only, never a dependency of the program.

use std::path::Path;
use std::process::{Command, Stdio};

/// `true` when the test must be skipped (`FS_STREAMING_NO_FFMPEG=1`); otherwise the tools must exist.
pub fn skip() -> bool {
    if std::env::var("FS_STREAMING_NO_FFMPEG").as_deref() == Ok("1") {
        eprintln!("SKIPPED: FS_STREAMING_NO_FFMPEG=1, this test needs ffmpeg and ffprobe");
        return true;
    }
    for tool in ["ffmpeg", "ffprobe"] {
        let ok = Command::new(tool)
            .arg("-version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
        assert!(
            ok,
            "{tool} is needed for this test; install it or set FS_STREAMING_NO_FFMPEG=1"
        );
    }
    false
}

/// A short test video with audio: H.264 (OpenH264) and AAC, a keyframe every second.
pub fn make_video(path: &Path, secs: u32) {
    make_video_with(path, secs, &["-c:v", "libopenh264", "-g", "25"]);
}

/// The same with other video encoder options (for example AV1, which FLV carries as enhanced RTMP).
pub fn make_video_with(path: &Path, secs: u32, video: &[&str]) {
    let lavfi = format!("testsrc=duration={secs}:size=320x240:rate=25");
    let sine = format!("sine=frequency=440:duration={secs}");
    let st = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            &lavfi,
            "-f",
            "lavfi",
            "-i",
            &sine,
        ])
        .args(video)
        .args(["-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest"])
        .arg(path)
        .status()
        .unwrap();
    assert!(st.success(), "ffmpeg could not make the test video");
}

/// Publish `file` to `url` in real time, copying the codecs.
pub fn spawn_publish(file: &Path, url: &str) -> tokio::process::Child {
    tokio::process::Command::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error", "-re", "-i"])
        .arg(file)
        .args(["-c", "copy", "-f", "flv", url])
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

#[derive(Debug)]
pub struct Probe {
    pub video: Option<String>,
    pub audio: Option<String>,
    pub duration: f64,
    /// Time of the first packet in seconds.
    pub start: f64,
}

pub fn probe(file: &Path) -> Probe {
    let out = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name,codec_type:format=duration,start_time",
            "-of",
            "compact",
        ])
        .arg(file)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    let mut p = Probe {
        video: None,
        audio: None,
        duration: 0.0,
        start: 0.0,
    };
    for line in text.lines() {
        let field = |k: &str| {
            line.split('|')
                .find_map(|f| f.strip_prefix(k))
                .map(str::to_string)
        };
        match (field("codec_type="), field("codec_name=")) {
            (Some(t), Some(n)) if t == "video" => p.video = Some(n),
            (Some(t), Some(n)) if t == "audio" => p.audio = Some(n),
            _ => {}
        }
        if let Some(d) = field("start_time=") {
            p.start = d.parse().unwrap_or(0.0);
        }
        if let Some(d) = field("duration=") {
            p.duration = d.parse().unwrap_or(0.0);
        }
    }
    p
}

/// What `ffmpeg` complains about while decoding the whole file; empty when it decodes cleanly.
pub fn decode_errors(file: &Path) -> String {
    let out = Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(file)
        .args(["-f", "null", "-"])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stderr).trim().to_string()
}

//! `fs-streaming`: the one program of FreeSynergy.Streaming (ADR 0063).
//!
//! Pattern: Facade — the default command is the window (`fs-streaming-app`); the words after the program name are
//! the window's options. There is no other command yet; the server (Cap'n Proto) is not built.

fn main() {
    let words: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(fs_streaming_app::app_command(&words));
}

# FreeSynergy Streaming — show something, and bring it everywhere

**Record once, send it to many places, and keep your stream key secret.**

FreeSynergy Streaming takes one live stream and sends it on to several targets at the same moment, without decoding it, and records it. You do not need a provider in the middle.

What is different here: the stream is passed on untouched, so quality is not lost and the machine does little work. A slow or dead target never slows the sender or the other targets. Keys are treated as secrets: the key you publish with is shown once and only a hash is kept, and keys never appear in logs or errors. If the sender drops, targets stay connected for a while and the stream continues when it returns.

## What it can do today

Latest tag: `v0.2.0` (stage 1 of the design).

- Receive one RTMP stream and send it to many RTMP or RTMPS targets.
- Record the stream to a folder as FLV.
- Check a target before going live, as far as that can be known.
- Tests: 94, with real sockets, using `ffmpeg` and `ffprobe` as test tools.
- Try it: `cargo run --example relay -- rtmp://host/app --key-env KEY` prints where to send and the one-time key, then forwards what it receives.

## What is not there yet

- Tested only on loopback with ffmpeg, never against a real streaming platform.
- No window, no server. Receiving is plain RTMP and stays on loopback by default.
- Handing a recording to Files is not built. Everything else in the contract is reported as "not possible".
- A forwarding loop through another machine cannot be detected from here.
- The interface is a draft, marked provisional.

## Part of FreeSynergy

Streaming is the last step in the plan, in the media suite next to files. FreeSynergy is a family of programs that are free for everyone, forever, and that keep your data yours. The documentation, the manifesto and the overall plan live in [fs-documentation](https://github.com/FreeSynergy/fs-documentation); the reading version is at <https://freesynergy.github.io/fs-documentation/>. Start with the [manifesto](https://freesynergy.github.io/fs-documentation/manifesto/) if you want to know why we do this.


## For developers

- Workspace crates: `fs-streaming-core` (streams, targets, keys as secrets, state; no media) and `fs-streaming-relay` (the forwarding, on `rml_rtmp` behind our own trait).
- The contract is `streaming.capnp` in [fs-interfaces](https://github.com/FreeSynergy/fs-interfaces) (draft, provisional). Streaming owns it.
- Build and test: `cargo build --workspace` and `cargo test --workspace`. Set `FS_STREAMING_NO_FFMPEG=1` to skip the tests that need ffmpeg.
- Design and decisions: [DESIGN.md](DESIGN.md).
- Licence: MIT OR Apache-2.0.

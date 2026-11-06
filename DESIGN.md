# fs-streaming – Design

Stage 1 of the design: **forward and record, no decoding.** No GUI, no capnp server. Contract: `streaming.capnp` in `fs-interfaces` v0.29.0 (draft, provisional; its header is binding). Program docs: `fs-documentation/docs/en/programs/apps/streaming/`.

## State

Version 0.1.0, being built. The State table is filled in as parts are finished.

## Decisions (the owner may change them; all provisional)

### Two crates
`fs-streaming-core` has the model, rules and storage, and no media or sockets, behind the port `Relay` (Strategy). `fs-streaming-relay` implements it: tokio tasks, RTMP through `rml_rtmp` (MIT, sans-I/O sessions) behind our own trait (Adapter), so it can be swapped.

### Storage and secrets
Streams and targets are written with `UnloggedObjects`, in a store the host passes and never connects to the hub: they hold or sit beside secrets, and an ordinary write would keep them in the log. Publisher key: 256 random bits from `getrandom`, shown once; only `sha256` is kept; compared in constant time. Target key: stored as given, because it has to be sent, and only in that store; there is no read path. The host decides on encryption at rest, and the store refuses nothing here, so **the DESIGN and the interface say that a target key is only as safe as this file**. No key vault exists yet (ADR 0024).
* Keys appear in no error, note, `Debug`, `tracing` line or feed event. `Debug` on the types holding them prints `[hidden]`; a test greps all outputs with a marker key.
* Notes are fixed wording of our own, by an enum; never far-end text.
* A target pointing at this relay's own address is refused, including through a wildcard bind. A loop through another machine cannot be seen from here; stated.
* A target URL is parsed strictly: `rtmp` or `rtmps`, host, optional port, an application path; no user, query or fragment; a percent sign in the path, backslashes, and a host with brackets that are not one exact IPv6 literal are refused; at most 2 KiB. A path containing the key cannot be told apart, so the interface asks for it separately; stated. Changing the host or going from rtmps to rtmp clears the key.

### State
State pattern, one transition function per stream and per target, as in the contract's header. `interrupted`: the sender dropped; for 30 s (a constant, provisional) targets stay connected and the recording stays open; a sender coming back continues; after that, `ended`. The newest sender with the right key wins. Wrong key: the same refusal as an unknown stream, with the same reads and the same hashing, so time does not tell them apart. An attempt limit per source address is not built; stated.

### The relay
Pipeline: ingest → one broadcast → one bounded queue per target and for recording. **A slow or dead target never slows the sender or the others.**
* What is forwarded: RTMP audio, video and data messages, payload untouched, timestamps kept; codec headers (the first sequence headers and metadata) kept in memory and sent first to anyone who joins, reconnects or starts recording. So enhanced RTMP passes through; to find a keyframe, only the frame-type bits of the first video byte are read, and for enhanced RTMP the same bits, per its open specification.
* A returning sender usually starts its timestamps over. An offset per sender keeps them rising for targets and recording; so do resent headers.
* Target queue full: frames are dropped for that target up to the next keyframe and counted in `dropped`; audio is dropped with its video, so they stay together. Queue limits are in bytes.
* Loss: reconnect with growing pauses, 1 s to 60 s with jitter, then join at the next keyframe. An explicit RTMP refusal makes `refused`: no more tries. That holds at any time during a connection. A server that accepts and then closes is a loss, and the pause grows unless media really flowed for a while.
* `rtmps` with `tokio-rustls` and `webpki-roots`, hostname checked; no way to switch that off. rustls's default crypto provider has C or assembly parts, like the other repos using rustls here; stated (ADR 0015).
* Limits at ingest: maximum chunk size and message size refused above 16 MiB, AMF messages above 8 KiB refused before parsing (nested values would overflow the stack), at most 64 connections, a deadline of 10 s from connecting to `publish`, an idle timeout and a write timeout, for sender and targets alike. `rml_rtmp` only ever sees commands and control messages, and a panic in a session ends only that connection and always releases the stream.
* Only media on the published stream id, after `publish`, is forwarded. Each message is handled in order, even in one read. Default bind is `127.0.0.1`, a host chooses otherwise. **Ingest is plain RTMP: its key travels readable**, so a host exposing it beyond the machine needs its own TLS in front; the interface says so.
* Clean ending: `stop`, `rotateKey`, disable and delete end their tasks; test for no task left behind.

### `check`
Connects, does TLS, RTMP handshake, `connect`, and only then, with a key, a `publish` that is ended at once, before any media. Result as in the contract: `reached` and `key` (`rejected` only on an explicit refusal, `accepted` only if the server said yes, else `unknown`). A note warns if the key travelled over plain `rtmp`. Honest limit: a server may accept and refuse later, and a platform may count it as a start of a broadcast; stated in the interface. No media is sent. Refused while live.

### Recording
Pattern Strategy: port `RecordingSink`, `create(info) -> writer`, `finish`. `FolderSink` writes FLV into a host-given folder under a generated name; own small FLV writer (header, tags, previous-tag sizes), no decoding. FLV holds what RTMP carried, enhanced included. A failing sink sets `recordingProblem` and never stops the stream. The problem stays until recording works again, and `setRecording(true)` starts a new recorder. Handing over to fs-files comes as another sink; its contract is provisional. `fileId` stays unset until then.

### Promises not used yet
`error` is entered only when the relay cannot listen, and `Stream.note` says why; other problems are per target or in `recordingProblem`. `recording.start` reports unsupported when no sink is configured.

### Not built
Everything listed in the contract's root comment. `Status` is live only, nothing of it is stored.

## Tests
Real sockets and real tools. `ffmpeg` and `ffprobe` are test-only tools, never a dependency of the program: a test publishes a generated video and audio to the real relay, with two local targets being further relay instances, and `ffprobe` reads what arrives and the recording. A test that needs them says so plainly and fails if they are absent, unless `FS_STREAMING_NO_FFMPEG=1`, which is then reported as skipped.

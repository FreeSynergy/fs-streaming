# fs-streaming – Design

Stage 1 of the design: **forward and record, no decoding.** A window (the generated admin frontend, ADR 0065/0066) over the core in the same process; no capnp server. Contract: `streaming.capnp` in `fs-interfaces` v0.29.0 (draft, provisional; its header is binding). Program docs: `fs-documentation/docs/en/programs/apps/streaming/`.

## State

Version 0.1.0. Stage 1 of the design; no capnp server, no GUI. **Tested only on loopback with `ffmpeg`, never against a real platform.**

| Area | State |
|---|---|
| Streams, targets, keys as secrets, state machines | done |
| RTMP ingest, one sender per stream, `interrupted` | done; plain RTMP, stays on loopback by default |
| Forwarding to RTMP and RTMPS targets, no decoding, enhanced RTMP passes through | done |
| `check` | done; can only know an explicit refusal |
| FLV recording through `RecordingSink`, `FolderSink` | done; handing over to fs-files not built |
| Type description `Stream`, `Target` (`DescribesType`, `fs-streaming-core/src/describe`) | done on `fs-types` v0.0.11 (`supported`, `Edit`); every capability id of `requirements.md` is in one table (43 ids); the app runs the supported rows |
| Window `fs-streaming` / `fs-streaming-app`: window shell + generated admin frontend, one view per type | done; Backends over the core in the same process, list = `find` with an empty query (paging by cursor) |
| `Streaming.types` (`TypeCatalog`) in `streaming.capnp` | contract done (`fs-interfaces` v0.0.10); **no capnp server exists**, so nothing answers it yet |
| Everything else in the contract's root comment | not built, reported unsupported |
| Tests | core and relay as before (real sockets, `ffmpeg`, `ffprobe`) plus `describe`, `backend`, `window`, `words` |

Checked by independent reviewers over several rounds before the tag.

## Decisions (the owner may change them; all provisional)

### Four crates
`fs-streaming-core` has the model, rules and storage, and no media or sockets, behind the port `Relay` (Strategy). `fs-streaming-relay` implements it: tokio tasks, RTMP through `rml_rtmp` (MIT, sans-I/O sessions) behind our own trait (Adapter), so it can be swapped. `fs-streaming-app` is the window: the standard shell of ADR 0065 around `fs-admin-frontend` (git `rev` of `fs-components`, provisional until it is a tag), the two Backends, the English words, About. `fs-streaming` is the binary; the default command is the window. No drawing code and no look in any of them (ADR 0066).

### Storage and secrets
Streams and targets are written with `UnloggedObjects`, in a store the host passes and never connects to the hub: they hold or sit beside secrets, and an ordinary write would keep them in the log. Publisher key: 256 random bits from `getrandom`, shown once; only `sha256` is kept; compared in constant time. Target key: stored as given, because it has to be sent, and only in that store; there is no read path. The host decides on encryption at rest, and the store refuses nothing here, so **the DESIGN and the interface say that a target key is only as safe as this file**. No key vault exists yet (ADR 0024).
* Keys appear in no error, note, `Debug`, `tracing` line or feed event. `Debug` on the types holding them prints `[hidden]`; a test greps all outputs with a marker key.
* Notes are fixed wording of our own, by an enum; never far-end text.
* A target pointing at this relay's own address is refused, including through a wildcard bind. A loop through another machine cannot be seen from here; stated.
* A target URL is parsed strictly: `rtmp` or `rtmps`, host, optional port, an application path; no user, query or fragment; a percent sign in the path, backslashes, and a host with brackets that are not one exact IPv6 literal are refused; at most 2 KiB. A path containing the key cannot be told apart, so the interface asks for it separately; stated. Changing the host or going from rtmps to rtmp clears the key.

### State
State pattern, one transition function per stream and per target, as in the contract's header. `interrupted`: the sender dropped; for 30 s (a constant, provisional) targets stay connected and the recording stays open; a sender coming back continues; after that, `ended`. The newest sender with the right key wins. Wrong key: the same refusal as an unknown stream, with the same reads and the same hashing. A small difference in time remains (measured about 1 ms, with many targets); it can show whether a stream id exists, not its key, and ids are not secret. An attempt limit per source address is not built; stated.

### The relay
Pipeline: ingest → one broadcast → one bounded queue per target and for recording. **A slow or dead target never slows the sender or the others.**
* What is forwarded: RTMP audio, video and data messages, payload untouched, timestamps kept; codec headers (the first sequence headers and metadata) kept in memory and sent first to anyone who joins, reconnects or starts recording. So enhanced RTMP passes through; to find a keyframe, only the frame-type bits of the first video byte are read, and for enhanced RTMP the same bits, per its open specification.
* A returning sender usually starts its timestamps over. An offset per sender keeps them rising for targets and recording; so do resent headers.
* Target queue full: frames are dropped for that target up to the next keyframe and counted in `dropped`; audio is dropped with its video, so they stay together. Queue limits are in bytes.
* Loss: reconnect with growing pauses, 1 s to 60 s with jitter, then join at the next keyframe. An explicit RTMP refusal makes `refused`: no more tries. That holds at any time during a connection. A server that accepts and then closes is a loss, and the pause grows unless media really flowed for a while.
* `rtmps` with `tokio-rustls` and `webpki-roots`, hostname checked; no way to switch that off. rustls's default crypto provider has C or assembly parts, like the other repos using rustls here; stated (ADR 0015).
* Limits at ingest: maximum chunk size and message size refused above 16 MiB, AMF messages above 8 KiB refused before parsing (nested values would overflow the stack), at most 64 connections that have not yet published (a publishing sender gives its place back), a deadline of 10 s from connecting to `publish`, also for one that stops reading, an idle timeout and a write timeout, for sender and targets alike. `rml_rtmp` only ever sees commands and control messages, and a panic in a session ends only that connection and always releases the stream.
* Only media on the published stream id, after `publish`, is forwarded. Each message is handled in order, even in one read. Default bind is `127.0.0.1`, a host chooses otherwise. **Stated limit: someone who can reach the port can keep 64 places busy, and a real sender then cannot get in until they expire.** Keep it on loopback or behind something that limits per address. **Ingest is plain RTMP: its key travels readable**, so a host exposing it beyond the machine needs its own TLS in front; the interface says so.
* Clean ending: `stop`, `rotateKey`, disable and delete end their tasks; test for no task left behind.

### `check`
Connects, does TLS, RTMP handshake, `connect`, and only then, with a key, a `publish` that is ended at once, before any media. Result as in the contract: `reached` and `key` (`rejected` only on an explicit refusal, `accepted` only if the server said yes, else `unknown`). A note warns if the key travelled over plain `rtmp`. Honest limit: a server may accept and refuse later, and a platform may count it as a start of a broadcast; stated in the interface. No media is sent. Refused while live.

### Recording
Pattern Strategy: port `RecordingSink`, `create(info) -> writer`, `finish`. `FolderSink` writes FLV into a host-given folder under a generated name; own small FLV writer (header, tags, previous-tag sizes), no decoding. FLV holds what RTMP carried, enhanced included. A failing sink sets `recordingProblem` and never stops the stream. The problem stays until recording works again, and `setRecording(true)` starts a new recorder. Handing over to fs-files comes as another sink; its contract is provisional. `fileId` stays unset until then.

### Promises not used yet
`error` is entered only when the relay cannot listen, and `Stream.note` says why; other problems are per target or in `recordingProblem`. `recording.start` reports unsupported when no sink is configured.

### Open points
* Acknowledgements are decided before a read is parsed: a sender that sends a window plus a whole window in one read and then waits gets no acknowledgement and is closed at 30 s.
* A raw acknowledgement on chunk stream 2 between two ping replies can desynchronise a sender's parser. Neither is known to be sent by ffmpeg or OBS.

### Not built
Everything listed in the contract's root comment. `Status` is live only, nothing of it is stored.

## The type description and the window (Thread R, 2026-11-10)

**Pattern and trait (ADR 0043), one sentence each.** The description: Registry entry, the trait is `DescribesType` of `fs-types` v0.0.2 (the same tag the admin frontend uses, so both name one `TypeDescription`), implemented by `Stream` and `Target` in `describe/`. The commands: ONE table (`describe/table.rs`, rows built in `stream_rows`, `planned_rows`, `target_rows`), a Registry of rows (name, kind, input, result, `supported`, reason). The Backends: Adapter to `Backend` of the frontend plus Command (one arm per row). The link from the synchronous frontend to the async core: Adapter (`CoreLink`, blocks the window thread for one call). Words: Strategy behind `Say`, one English table.

**Standards.** No data standard covers a live stream (`docs/en/data/` has no streaming domain). `TypeDescription.standard` says so: `none (own type, streaming.capnp; RTMP ingest)` for `Stream`, `... RTMP and RTMPS addresses` for `Target`. Fields use basic types of `fs-types` (text, link, choice, reference, date-time). Ids are `urn:uuid:<uuid>`.

**All commands, supported or not.** `describe()` reports every row of the table, supported or not (a frontend can only show "coming soon" for what the description carries). `fs-types` v0.0.11 has `CommandDescription.supported` (unset on the wire = false). **The switch is one function, `to_command` in `describe/table.rs`, marked `// SWITCH`:** a row with `supported = true` becomes a plain command, one with `false` becomes `.unsupported()`. To free a command, flip `supported` in its row (and remove its `.planned(..)`); nothing else changes, not the frontend. The table stays as the source of the reasons and of the capability ids: a test fails when it lacks an id of `requirements.md` (`tests/data/capability-ids.txt`; with `FS_REQUIREMENTS_MD=<file>` it is compared with the live file) or has one twice, and one that checks each flag in the description against its row. The Backend answers `unsupported:<name>` for a row that is not supported (as `BackendError::Unavailable`; `Backend::supported(name)` says false), as a second line behind the lock, not as the display.

**Rules of `types.capnp` (2026-11-10) followed.** List = the standard `find` (`CommandDescription::standard_find`, input `term`, empty = all; the app filters by a part of the name, case-insensitive; `Target.find` has a further input `stream`; paging by cursor is the core's). Change = kind `change` with no input, an `Edit` (`Backend::change(record, edit)`): missing = unchanged, no values = cleared, values replace; the backend runs `Edit::check` itself and refuses, before it calls the core, any field the core owns (`state`, `note`, `ingestUrl`, `hasKey`, `secure`, `stream`) as a finding `UnknownField` (there is no read-only finding in `fs-types`), and the clearing of a required field as `Missing`. Changeable: `Stream.name`, `Stream.recording`, `Target.name`, `Target.url`, `Target.enabled` (the core takes name and address together; the one left out is read from the target). Record commands: `TypeDescription::record_commands` (a reference to the own type in the input, plus change and delete); the record id comes in `run(name, record, input)`.

**Keys are not in any record.** `Stream` has no key field; `Target` has the flag `hasKey` and no key. A test greps records, results and the About for a key made by the test as a marker.

**Provisional choices (reported).** There is no boolean basic type: flags are `choice` of two keys (`on`/`off`, `yes`/`no`). A count (`bytesIn`) is `text`. Notes are the core's fixed English sentences (`Note::text`), not keys. The About says origin `source`: the store passes the program nothing yet. `windows-targets: "x86_64"` in `release.yml`: no Unix-only code is in the app (the private store folder is made with mode 0700 on Unix only), but it was never built for Windows; the first release run shows it.

## Unsupported (named, not faked)

* **`create` and `rotateKey` (Stream): held back by the app, not by the core.** The publisher key is shown once and must never be in a field value, record or About. The frontend has no one-time secret display, so the app does not run them: until the Strategy decides how a key is shown once (a secret kind in `fs-types`, or a hand-over that stays outside records), **a stream cannot be made from the window**. The core and `Streams` can.
* **`setKey` (Target) and the key of `add`:** the same for a target key: no masked input that keeps the text out of the form model. `add` takes name and address and no key; `setKey` is not run.
* **All commands of `requirements.md` that the core cannot do** (ingest by SRT/RTSP/WHIP/RTMPS, playback, remux, pull, viewers, hooks, recordings listing, everything that decodes or encodes, guests, fediverse, ...): rows of the table with `supported = false` and a reason. Not built; see "Not built" below.
* **Serving `Streaming.types` over Cap'n Proto:** the contract has it (`fs-interfaces` v0.0.10), but this program has no capnp server yet.
* **BLOCKER: the frontend rev.** `fs-types` v0.0.11 is used here; the pushed `fs-admin-frontend` (`4688cee`, and the branch head `5809e89`) is on v0.0.2 and has neither `Backend::change` nor `run(name, record, input)`. The app crate was written and tested (all `fs-streaming-app` tests green) against Thread A's uncommitted worktree through a local path patch; its `Cargo.toml` still names `4688cee` and the branch `r4-admin-v11` does not build the app until that rev is replaced by the pushed commit on v0.0.11 (four lines, marked BLOCKED). Branch `r4-admin` is the green state on v0.0.2 (gates ok, local table as the switch, `rename`/`edit` as commands).
* With the locks in the frontend, six list-level commands do not fit the head of the list at 1100 px (found in a picture, `--snapshot`): the frontend's to fix.
* **Windows:** see above, untested.

## Pattern map

| Where | Pattern | Why |
|---|---|---|
| `core/describe/` (`Stream`, `Target`) | Registry entry, trait `DescribesType` | the description is what the frontend generates from |
| `core/describe/table.rs`, `*_rows.rs`, `row.rs` | Registry + Builder | one table of every command, one switch (`to_command`) for `supported` |
| `app/edits.rs` | Adapter | an `Edit` read field by field, checked, read-only fields refused |
| `core/service`, `core/repo`, `core/state`, `core/traits`, `core/model` | Facade, Repository, State, Strategy, Observer (as in each module head) | the core, unchanged by Thread R |
| `relay/` | Strategy (`Relay`, `RecordingSink`), Adapter (`rml_rtmp`) | unchanged by Thread R |
| `app/streams.rs`, `app/targets.rs` | Adapter to `Backend` + Command | one arm per row of the table |
| `app/core_link.rs` | Adapter | sync frontend, async core |
| `app/records.rs`, `app/errors.rs`, `app/ids.rs`, `app/values.rs` | Adapter | core values to the frontend's records, errors, ids, input |
| `app/words.rs`, `app/words_labels.rs` | Strategy (`Say`) + Registry | English now, translation later replaces the table |
| `app/open.rs` | Facade | store, core and relay in this process |
| `app/window.rs` | Facade | `admin_window` with one `TypeView` per type |
| `app/about.rs`, `app/args.rs` | Value object | About from the package; plain options, no key on the command line |
| `fs-streaming/main.rs` | Facade | the default command is the window |

## Tests
Real sockets and real tools. `ffmpeg` and `ffprobe` are test-only tools, never a dependency of the program: a test publishes a generated video and audio to the real relay, with two local targets being further relay instances, and `ffprobe` reads what arrives and the recording. A test that needs them says so plainly and fails if they are absent, unless `FS_STREAMING_NO_FFMPEG=1`, which is then reported as skipped.
